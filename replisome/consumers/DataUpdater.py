from operator import itemgetter

import psycopg2.extras
from psycopg2 import extensions as ext
from psycopg2 import sql

from replisome.errors import ReplisomeError

import logging
logger = logging.getLogger('replisome.DataUpdater')


def tupgetter(*idxs):
    """Like itemgetter, but return a 1-tuple if the input is one index."""
    if len(idxs) == 0:
        return tuple

    if len(idxs) == 1:
        def tupgetter_(obj, _idx=idxs[0]):
            return (obj[_idx],)

        return tupgetter_

    else:
        return itemgetter(*idxs)


class DataUpdater(object):
    def __init__(self, dsn, upsert=False,
                 skip_missing_columns=False, skip_missing_tables=False):
        """
        Apply changes to a database receiving message from a replisome stream.

        :arg upsert: If true update instead (only on primary key, TODO on other
             fields)
        :arg skip_missing_columns: If true drop the values in the messages
            for columns not available locally; otherwise fail if such columns
            are found.
        :arg skip_missing_columns: If true records on non existing tables are
            dropped.
        """
        self.dsn = dsn
        self.upsert = upsert
        self.skip_missing_columns = skip_missing_columns
        self.skip_missing_tables = skip_missing_tables
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
            for ch in msg['tx']:
                self.process_change(cnn, ch)

            cnn.commit()
        finally:
            self.put_connection(cnn)

    def __call__(self, msg):
        self.process_message(msg)

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
            if not self.skip_missing_tables:
                raise ReplisomeError(
                    "received insert on table %s.%s not available" % (s, t))

            logger.info("received insert on table %s.%s not available", s, t)
            return None, None

        local_cols = set(local_cols)
        msg_cols = self._colnames[self.key(msg)]
        if not self.skip_missing_columns:
            missing = set(msg_cols) - local_cols
            if missing:
                raise ReplisomeError(
                    "insert message on table %s.%s has columns not available "
                    "locally: %s" % (s, t, ', '.join(sorted(missing))))

        idxs = [i for i, c in enumerate(msg_cols) if c in local_cols]

        if self.upsert:
            if cnn.server_version < 90500:
                raise ReplisomeError(
                    "upsert is only available from PostgreSQL 9.5")

            key_cols = self.get_table_pkey(cnn, s, t)
            if key_cols is None:
                logger.info("table %s.%s can't have upsert: no primary key")
                return None, None

            nokeyidxs = [i for i, c in enumerate(msg_cols)
                        if c in local_cols and c not in key_cols]
        else:
            nokeyidxs = None

        if not idxs:
            logger.info(
                "the local table has no field in common with the message")
            return None, None

        logger.debug(
            "the local table has %d field in common with the message",
            len(idxs))
        colmap = tupgetter(*idxs)

        def acc(msg, _map=tupgetter(*(idxs + (nokeyidxs or [])))):
            return _map(msg['values'])

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

        if self.upsert:
            bits.append(sql.SQL(' on conflict ('))
            bits.append(sql.SQL(',').join(map(sql.Identifier, key_cols)))
            if nokeyidxs:
                bits.append(sql.SQL(') do update set ('))
                bits.append(sql.SQL(',').join(map(
                    sql.Identifier, tupgetter(*nokeyidxs)(msg_cols))))
                bits.append(sql.SQL(') = ('))
                bits.append(sql.SQL(',').join(sql.Placeholder() * len(nokeyidxs)))
                bits.append(sql.SQL(')'))
            else:
                bits.append(sql.SQL(') do nothing'))

        stmt = sql.Composed(bits).as_string(cnn)

        logger.debug("generated query: %s", stmt)

        return stmt, acc

    def make_update(self, cnn, msg):
        """
        Return the query and message-to-argument function to perform an update.
        """
        s = msg.get('schema')
        t = msg['table']
        local_cols = self.get_table_columns(cnn, s, t)
        if local_cols is None:
            if not self.skip_missing_tables:
                raise ReplisomeError(
                    "received update on table %s.%s not available" % (s, t))

            logger.debug("received update on table %s.%s not available", s, t)
            return None, None

        local_cols = set(local_cols)
        msg_cols = self._colnames[self.key(msg)]
        msg_keys = self._keynames[self.key(msg)]

        if not self.skip_missing_columns:
            missing = set(msg_cols) - local_cols
            if missing:
                raise ReplisomeError(
                    "update message on table %s.%s has columns not available "
                    "locally: %s" % (s, t, ', '.join(sorted(missing))))

        # the key must be entirely known
        kidxs = [i for i, c in enumerate(msg_keys) if c in local_cols]
        if not(kidxs):
            raise ReplisomeError("the table %s.%s has no key" % (s, t))

        if len(kidxs) != len(msg_keys):
            raise ReplisomeError(
                "the local table %s.%s is missing some key fields %s" %
                (s, t, msg_keys))

        idxs = [i for i, c in enumerate(msg_cols) if c in local_cols]
        if not idxs:
            logger.info(
                "the local table has no field in common with the message")
            return None, None

        colmap = tupgetter(*idxs)
        keymap = tupgetter(*kidxs)

        logger.debug(
            "the local table has %d field in common with the message",
            len(idxs))

        def acc(msg, _colmap=colmap, _keymap=keymap):
            return _colmap(msg['values']) + _keymap(msg['oldkey'])

        cols = colmap(msg_cols)
        keycols = keymap(msg_keys)

        bits = [sql.SQL('update ')]
        if 'schema' in msg:
            bits.append(sql.Identifier(msg['schema']))
            bits.append(sql.SQL('.'))
        bits.append(sql.Identifier(msg['table']))
        bits.append(sql.SQL(' set ('))
        bits.append(sql.SQL(',').join(map(sql.Identifier, cols)))
        bits.append(sql.SQL(') = ('))
        bits.append(sql.SQL(',').join(sql.Placeholder() * len(cols)))
        bits.append(sql.SQL(') where ('))
        bits.append(sql.SQL(',').join(map(sql.Identifier, keycols)))
        bits.append(sql.SQL(') = ('))
        bits.append(sql.SQL(',').join(sql.Placeholder() * len(keycols)))
        bits.append(sql.SQL(')'))

        stmt = sql.Composed(bits).as_string(cnn)

        logger.debug("generated query: %s", stmt)

        return stmt, acc

    def make_delete(self, cnn, msg):
        """
        Return the query and message-to-argument function to perform a delete.
        """
        s = msg.get('schema')
        t = msg['table']
        local_cols = self.get_table_columns(cnn, s, t)
        if local_cols is None:
            if not self.skip_missing_tables:
                raise ReplisomeError(
                    "received delete on table %s.%s not available" % (s, t))

            logger.debug("received delete on table %s.%s not available", s, t)
            return None, None

        local_cols = set(local_cols)
        msg_keys = self._keynames[self.key(msg)]

        # the key must be entirely known
        kidxs = [i for i, c in enumerate(msg_keys) if c in local_cols]
        if not(kidxs):
            raise ReplisomeError("the table %s.%s has no key" % (s, t))

        if len(kidxs) != len(msg_keys):
            raise ReplisomeError(
                "the local table %s.%s is missing some key fields %s" %
                (s, t, msg_keys))

        keymap = tupgetter(*kidxs)

        def acc(msg, _keymap=keymap):
            return _keymap(msg['oldkey'])

        keycols = keymap(msg_keys)

        bits = [sql.SQL('delete from ')]
        if 'schema' in msg:
            bits.append(sql.Identifier(msg['schema']))
            bits.append(sql.SQL('.'))
        bits.append(sql.Identifier(msg['table']))
        bits.append(sql.SQL(' where ('))
        bits.append(sql.SQL(',').join(map(sql.Identifier, keycols)))
        bits.append(sql.SQL(') = ('))
        bits.append(sql.SQL(',').join(sql.Placeholder() * len(keycols)))
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

    def get_table_pkey(self, cnn, schema, table):
        """
        Return the list of column names in a table's primary key, optionally
        schema-qualified

        Return null if the table is not found.
        """
        if schema is None:
            sql = """
                select array_agg(attname)
                from (
                    select attname from pg_attribute
                    where attrelid = (
                        select c.conindid from pg_class r
                        join pg_constraint c on conrelid = r.oid
                        where r.relname = %(table)s
                        and r.relkind = 'r' and contype = 'p' limit 1)
                    and attnum > 0 and not attisdropped
                    order by attnum) x
                """
        else:
            sql = """
                select array_agg(attname)
                from (
                    select attname from pg_attribute
                    where attrelid = (
                        select c.conindid from pg_class r
                        join pg_constraint c on conrelid = r.oid
                        join pg_namespace s on s.oid = r.relnamespace
                        where r.relname = %(table)s and nspname = %(schema)s
                        and r.relkind = 'r' and contype = 'p' limit 1)
                    and attnum > 0 and not attisdropped
                    order by attnum) x
                """
        cur = cnn.cursor()
        cur.execute(sql, {'table': table, 'schema': schema})
        return cur.fetchone()[0]

    def key(self, msg):
        """Return a key to identify a table from a message."""
        return (msg.get('schema'), msg['table'])
