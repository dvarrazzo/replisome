#!/usr/bin/env python

import sys
import json
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
        if message_cb:
            self.message_cb = message_cb

        self._chunks = []

    def start(self, create=False):
        self.connection = self.create_connection()

        cur = self.connection.cursor()

        if create:
            logger.info('creating replication slot "%s"', self.slot)
            cur.create_replication_slot(self.slot, output_plugin=self.plugin)

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
            pos = 0
            logger.info(
                'starting streaming from slot "%s" at %s', self.slot, pos)
            cur.start_replication(
                self.slot, start_lsn=pos, decode=False,
                options={'pretty-print': '0', 'write-in-chunks': '1'})
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

    def consume(self, msg):
        cnn = msg.cursor.connection
        if cnn.notices:
            for n in cnn.notices:
                logger.debug("server: %s", n.rstrip())
            del cnn.notices[:]

        logger.debug(
            u"message received:\n\t%r\n\t%s%s",
            msg, msg.payload[:50], len(msg.payload) > 50 and '...' or '')
        chunk = msg.payload
        self._chunks.append(chunk)

        if chunk == ']}' or chunk == '\t]\n}':
            obj = json.loads(''.join(self._chunks))
            del self._chunks[:]
            self.message_cb(obj)

        logger.debug("sending feedback: %X", msg.data_start)
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
        self.dsn = dsn
        self._connection = None

        self._colnames = {}
        self._keynames = {}
        self._insert_stmts = {}
        self._update_stmts = {}
        self._delete_stmts = {}

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
        cnn = self.get_connection()
        try:
            for ch in msg['change']:
                op = ch['op']
                if op == 'I':
                    self.process_insert(cnn, ch)
                elif op == 'U':
                    self.process_update(cnn, ch)
                elif op == 'D':
                    self.process_delete(cnn, ch)
                else:
                    raise ValueError("unknown operation: '%s'" % op)

            cnn.commit()
        finally:
            self.put_connection(cnn)

    def process_insert(self, cnn, msg):
        stmt = self.get_insert_stmt(cnn, msg)
        cur = cnn.cursor()
        logger.debug("inserting values: %s", msg['values'])
        cur.execute(stmt, msg['values'])

    def get_insert_stmt(self, cnn, msg):
        t = msg['table']
        s = msg['schema']

        if 'colnames' in msg:
            logger.debug("got new schema for table %s.%s", s, t)
            self._colnames[s, t] = msg['colnames']
            self._insert_stmts.pop((s, t), None)

        try:
            return self._insert_stmts[s, t]
        except KeyError:
            colnames = self._colnames[s, t]
            stmt = sql.SQL("insert into {s}.{t} ({fs}) values ({vs})").format(
                s=sql.Identifier(s), t=sql.Identifier(t),
                fs=sql.SQL(', ').join(map(sql.Identifier, colnames)),
                vs=sql.SQL(', ').join(sql.Placeholder() * len(colnames)))
            stmt = stmt.as_string(cnn)
            logger.debug("new statement: %s", stmt)
            self._insert_stmts[s, t] = stmt
            return stmt


def main():
    opt = parse_cmdline()

    if opt.verbose:
        logger.setLevel(logging.DEBUG)

    sa = SchemaApplier(opt.tgt_dsn)
    rr = ReplisomeReceiver(
        opt.slot, opt.src_dsn, message_cb=sa.process_message)
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
        '--create-slot', action='store_true', default=False,
        help="create the slot before streaming")
    parser.add_argument(
        '--drop-slot', action='store_true', default=False,
        help="drop the slot after streaming")
    parser.add_argument(
        '--verbose', action='store_true',
        help="print more log messages")

    opt = parser.parse_args()

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
