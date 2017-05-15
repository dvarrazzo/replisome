import json

import psycopg2.extras
from psycopg2 import sql

import logging
logger = logging.getLogger('replisome.json_receiver')


class JsonReceiver(object):
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
