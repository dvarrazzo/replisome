#!/usr/bin/env python

import sys
import json
from operator import itemgetter

import psycopg2.extras
from psycopg2 import extensions as ext
from psycopg2 import sql

import logging
logging.basicConfig(
    level=logging.INFO, stream=sys.stderr,
    format="%(asctime)s %(levelname)s %(message)s")

logger = logging.getLogger()


class ScriptError(Exception):
    """Controlled exception raised by the script."""


class ReplisomeReceiver(object):
    def __init__(self, slot, dsn, plugin="replisome", message_cb=None):
        self.slot = slot
        self.dsn = dsn
        self.connection = None
        self.plugin = plugin
        self.options = []
        if message_cb:
            self.message_cb = message_cb

        self._chunks = []

    def start(self, create=False):
        cnn = self.connection = self.create_connection()
        cur = cnn.cursor()

        if create:
            logger.info('creating replication slot "%s"', self.slot)
            cur.create_replication_slot(self.slot, output_plugin=self.plugin)

        stmt = self.get_replication_statement(cnn)

        # NOTE: it is currently necessary to write in chunk even if this
        # results in messages containing invalid JSON (which are assembled
        # by consume()). If we don't do so, it seems postgres fails to flush
        # the lsn correctly. I suspect the problem is that we reset the lsn
        # at the start of the message: if the output is chunked we receive
        # at least a couple of messages within the same transaction so we
        # end up being correct. If the message encompasses the entire
        # transaction, the lsn is reset at the beginning of the transction
        # already seen, so some records are sent repeatedly.
        try:
            logger.info(
                'starting streaming from slot "%s"', self.slot)

            cur.start_replication_expert(stmt, decode=False)
            cur.consume_stream(self.consume, keepalive_interval=1)
        finally:
            self.close()

    def get_start(self):
        with psycopg2.connect(self.dsn) as cnn:
            cur = cnn.cursor()
            cur.execute("""
                select restart_lsn from pg_replication_slots
                where slot_name = %s
                """, (self.slot,))
            rec = cur.fetchone()
            if not rec:
                raise ScriptError(
                    "replication slot '%s' not found" % self.slot)
            return rec[0]

    def get_replication_statement(self, cnn):
        options = (
            [('write-in-chunks', '1')] +
            self.options)

        bits = [
            sql.SQL("START_REPLICATION SLOT "),
            sql.Identifier(self.slot) +
            sql.SQL(" LOGICAL 0/0")]

        if options:
            bits.append(sql.SQL(' ('))
            for k, v in options:
                bits.append(sql.Identifier(k))
                if v is not None:
                    bits.append(sql.SQL(' '))
                    bits.append(sql.Literal(v))
                bits.append(sql.SQL(', '))
            bits[-1] = sql.SQL(')')

        rv = sql.Composed(bits).as_string(cnn)
        logger.debug("replication statement: %s", rv)
        return rv

    def consume(self, msg):
        cnn = msg.cursor.connection
        if cnn.notices:
            for n in cnn.notices:
                logger.debug("server: %s", n.rstrip())
            del cnn.notices[:]

        logger.debug(
            "message received:\n\t%s%s",
            msg.payload[:70], len(msg.payload) > 70 and '...' or '')
        chunk = msg.payload
        self._chunks.append(chunk)

        if chunk == ']}' or chunk == '\t]\n}':
            obj = json.loads(''.join(self._chunks))
            del self._chunks[:]
            self.message_cb(obj)

        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    def message_cb(self, obj):
        logger.info("message received: %s", obj)

    def create_connection(self):
        logger.info('connecting to source database at "%s"', self.dsn)
        return psycopg2.connect(
            self.dsn,
            connection_factory=psycopg2.extras.LogicalReplicationConnection)

    def drop_slot(self):
        try:
            cnn = self.create_connection()
            cur = cnn.cursor()
            logger.info('dropping replication slot "%s"', self.slot)
            cur.drop_replication_slot(self.slot)
            cnn.close()
        except Exception as e:
            logger.error(
                "error dropping replication slot: %s", e)

    def close(self):
        if not self.connection:
            return
        try:
            self.connection.close()
        except Exception as e:
            logger.error(
                "error closing receiving connection - ignoring: %s", e)


class SchemaApplier(object):
    def __init__(self, dsn):
        """
        Apply changes to a database receiving message from a replisome stream.
        """
        self.dsn = dsn
        self._connection = None

        # Maps from the key() of the message to the columns and table key names
        self._colnames = {}
        self._keynames = {}

        # Maps from the key() of the message to the query to perform each
        # operation (insert, update, delete). The values are in the format
        # returned by _get_statement() and are invalidated when new colnames
        # or keynames are received in a message (suggesting a schema change
        # in the origin database).
        self._stmts = {'I': {}, 'U': {}, 'D': {}}

    def get_connection(self):
        cnn, self._connection = self._connection, None
        if cnn is None:
            cnn = self.connect()

        return cnn

    def put_connection(self, cnn):
        if cnn.closed:
            logger.info("discarding closed connection")
            return

        status = cnn.get_transaction_status()
        if status == ext.TRANSACTION_STATUS_UNKNOWN:
            logger.info("closing connection in unknown status")
            cnn.close()
            return

        elif status != ext.TRANSACTION_STATUS_IDLE:
            logger.warn("rolling back transaction in status %s", status)
            cnn.rollback()

        self._connection = cnn

    def connect(self):
        logger.info('connecting to target database at "%s"', self.dsn)
        cnn = psycopg2.connect(self.dsn)
        return cnn

    def process_message(self, msg):
        """
        Process an entire message returned by the source.

        Apply the message to the target database in a single transaction.
        """
        cnn = self.get_connection()
        try:
            for ch in msg['change']:
                self.process_change(cnn, ch)

            cnn.commit()
        finally:
            self.put_connection(cnn)

    def process_change(self, cnn, msg):
        """
        Process one of the changes in a replisome message.
        """
        stmt, acc = self._get_statement(cnn, msg)
        if stmt is None:
            logger.debug("skipping message on %s", msg['table'])
            return

        cur = cnn.cursor()
        try:
            cur.execute(stmt, acc(msg))
        except psycopg2.DatabaseError:
            logger.error("error running the query: %s", cur.query)
            raise
        logger.debug("query run: %s", cur.query)

    def _get_statement(self, cnn, msg):
        """
        Return the statement needed to process a change.

        The statement is a pair (sql, acc) where sql is a query string and
        acc is a function that takes the message as input and returns the
        query argument.
        """
        k = self.key(msg)
        if 'colnames' in msg:
            logger.debug("got new columns for table %s", k)
            if k in self._colnames:
                self._stmts['I'].pop(k, None)
                self._stmts['U'].pop(k, None)
            self._colnames[k] = msg['colnames']

        if 'keynames' in msg:
            logger.debug("got new key for table %s", k)
            if k in self._keynames:
                self._stmts['U'].pop(k, None)
                self._stmts['D'].pop(k, None)
            self._keynames[k] = msg['keynames']

        op = msg['op']
        stmts = self._stmts[op]
        try:
            rv = stmts[k]
        except KeyError:
            if op == 'I':
                rv = self.make_insert(cnn, msg)
            elif op == 'U':
                rv = self.make_update(cnn, msg)
            elif op == 'D':
                rv = self.make_delete(cnn, msg)

            stmts[k] = rv

        return rv

    def make_insert(self, cnn, msg):
        """
        Return the query and message-to-argument function to perform an insert.
        """
        s = msg.get('schema')
        t = msg['table']
        local_cols = self.get_table_columns(cnn, s, t)
        if local_cols is None:
            logger.debug("table %s.%s not available", s, t)
            return None, None

        local_cols = set(local_cols)
        msg_cols = self._colnames[self.key(msg)]
        idxs = [i for i, c in enumerate(msg_cols) if c in local_cols]
        if not idxs:
            logger.debug(
                "the local table has no field in common with the message")
            return None, None

        elif len(idxs) == len(msg_cols):
            logger.debug(
                "the local table has exactly the same fields of the message")

            def colmap(values):
                return values

            acc = itemgetter('values')

        elif len(idxs) == 1:
            logger.debug(
                "the local table has one field in common with the message")

            def colmap(values, i=idxs[0]):
                return (values[i],)

            def acc(msg, i=idxs[0]):
                return (msg['values'][i],)

        else:
            logger.debug(
                "the local table has %d field in common with the message",
                len(idxs))
            colmap = itemgetter(*idxs)

            def acc(msg, colmap=colmap):
                return colmap(msg['values'])

        cols = colmap(msg_cols)

        bits = [sql.SQL('insert into ')]
        if 'schema' in msg:
            bits.append(sql.Identifier(msg['schema']))
            bits.append(sql.SQL('.'))
        bits.append(sql.Identifier(msg['table']))
        bits.append(sql.SQL(' ('))
        bits.append(sql.SQL(',').join(map(sql.Identifier, cols)))
        bits.append(sql.SQL(') values ('))
        bits.append(sql.SQL(',').join(sql.Placeholder() * len(cols)))
        bits.append(sql.SQL(')'))
        stmt = sql.Composed(bits).as_string(cnn)

        logger.debug("generated query: %s", stmt)

        return stmt, acc

    def get_table_columns(self, cnn, schema, table):
        """
        Return the list of column names in a table, optionally schema-qualified

        Return null if the table is not found.
        """
        if schema is None:
            sql = """
                select array_agg(attname)
                from (
                    select attname from pg_attribute
                    where attrelid = (
                        select c.oid from pg_class c
                        where relname = %(table)s
                        and relkind = 'r' limit 1)
                    and attnum > 0 and not attisdropped
                    order by attnum) x
                """
        else:
            sql = """
                select array_agg(attname)
                from (
                    select attname from pg_attribute
                    where attrelid = (
                        select c.oid from pg_class c
                        join pg_namespace s on s.oid = relnamespace
                        where relname = %(table)s and nspname = %(schema)s
                        and relkind = 'r' limit 1)
                    and attnum > 0 and not attisdropped
                    order by attnum) x
                """
        cur = cnn.cursor()
        cur.execute(sql, {'table': table, 'schema': schema})
        return cur.fetchone()[0]

    def key(self, msg):
        """Return a key to identify a table from a message."""
        return (msg.get('schema'), msg['table'])


def main():
    opt = parse_cmdline()

    if opt.verbose:
        logger.setLevel(logging.DEBUG)

    sa = SchemaApplier(opt.tgt_dsn)
    rr = ReplisomeReceiver(
        opt.slot, opt.src_dsn, message_cb=sa.process_message)
    rr.options = opt.options
    try:
        rr.start(create=opt.create_slot)
    finally:
        if opt.drop_slot:
            rr.drop_slot()


def parse_cmdline():
    from argparse import ArgumentParser
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        'slot',
        help="slot name to use to receive data")
    parser.add_argument(
        'src_dsn',
        help="connection string to connect to and read data")
    parser.add_argument(
        'tgt_dsn',
        help="connection string to connect to and write data")
    parser.add_argument(
        '-o', metavar="KEY=VALUE", action='append', dest='options',
        default=[], help="pass some option to the decode plugin")
    parser.add_argument(
        '--create-slot', action='store_true', default=False,
        help="create the slot before streaming")
    parser.add_argument(
        '--drop-slot', action='store_true', default=False,
        help="drop the slot after streaming")
    parser.add_argument(
        '--verbose', action='store_true',
        help="print more log messages")

    opt = parser.parse_args()

    for i, o in enumerate(opt.options):
        try:
            k, v = o.split('=', 1)
        except ValueError:
            parser.error("bad option: %s" % o)
        else:
            opt.options[i] = (k, v)

    return opt

if __name__ == '__main__':
    try:
        sys.exit(main())

    except ScriptError, e:
        logger.error("%s", e)
        sys.exit(1)

    except Exception:
        logger.exception("unexpected error")
        sys.exit(1)

    except KeyboardInterrupt:
        logger.info("user interrupt")
        sys.exit(1)
