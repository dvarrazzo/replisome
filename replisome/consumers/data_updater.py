from operator import itemgetter

import psycopg2.extras
from psycopg2 import extensions as ext
from psycopg2 import sql

import logging
logger = logging.getLogger('replisome.postgres_user')


class DataUpdater(object):
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
