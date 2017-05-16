import os
import json
from select import select

import psycopg2
from psycopg2.extras import LogicalReplicationConnection, wait_select
from psycopg2 import sql

from replisome.errors import ConfigError

import logging
logger = logging.getLogger('replisome.JsonReceiver')


class JsonReceiver(object):
    def __init__(self, slot=None, dsn=None, message_cb=None,
            plugin="replisome", options=None):
        self.slot = slot
        self.dsn = dsn
        self.plugin = plugin
        self.options = options or []
        if message_cb:
            self.message_cb = message_cb

        self._shutdown_pipe = os.pipe()

        self._chunks = []

    @classmethod
    def from_config(cls, config):
        opts = []
        # TODO: make them the same (parse the underscore version in the plugin)
        for k in ('pretty_print', 'include_xids', 'include_lsn',
                  'include_timestamp', 'include_schemas', 'include_types',
                  'include_empty_xacts'):
            if k in config:
                v = config.pop(k)
                opts.append((k.replace('_', '-'), (v and 't' or 'f')))

        incs = config.pop('includes', [])
        if not isinstance(incs, list):
            raise ConfigError('includes should be a list, got %s' % (incs,))

        for inc in incs:
            # TODO: this might be parsed by the plugin
            k = inc.pop('exclude', False) and 'exclude' or 'include'
            opts.append((k, json.dumps(inc)))

        if config:
            raise ConfigError(
                "unknown %s option entries: %s" %
                (cls.__name__, ', '.join(sorted(config))))

        return cls(options=opts)

    def __del__(self):
        self.stop()

    def start(self, connection, create=False):
        if not self.slot:
            raise ValueError("no slot specified")

        if not connection.async_:
            raise ValueError("the connection should be asynchronous")

        cur = connection.cursor()

        if create:
            logger.info('creating replication slot "%s"', self.slot)
            cur.create_replication_slot(self.slot, output_plugin=self.plugin)
            wait_select(connection)

        stmt = self.get_replication_statement(connection)

        # NOTE: it is currently necessary to write in chunk even if this
        # results in messages containing invalid JSON (which are assembled
        # by consume()). If we don't do so, it seems postgres fails to flush
        # the lsn correctly. I suspect the problem is that we reset the lsn
        # at the start of the message: if the output is chunked we receive
        # at least a couple of messages within the same transaction so we
        # end up being correct. If the message encompasses the entire
        # transaction, the lsn is reset at the beginning of the transction
        # already seen, so some records are sent repeatedly.
        logger.info(
            'starting streaming from slot "%s"', self.slot)

        cur.start_replication_expert(stmt, decode=False)
        wait_select(connection)

        while 1:
            msg = cur.read_message()
            if msg:
                self.consume(msg)
                continue

            # TODO: handle InterruptedError
            sel = select(
                [self._shutdown_pipe[0], connection], [], [], 10)
            if not any(sel):
                cur.send_feedback()
            elif self._shutdown_pipe[0] in sel[0]:
                break

    def stop(self):
        os.write(self._shutdown_pipe[1], 'stop')

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

    def create_connection(self, async_=True):
        logger.info('connecting to source database at "%s"', self.dsn)
        cnn = psycopg2.connect(
            self.dsn, async_=async_,
            connection_factory=LogicalReplicationConnection)
        wait_select(cnn)
        return cnn

    def drop_slot(self):
        try:
            with self.create_connection(async_=False) as cnn:
                cur = cnn.cursor()
                logger.info('dropping replication slot "%s"', self.slot)
                cur.drop_replication_slot(self.slot)
        except Exception as e:
            logger.error(
                "error dropping replication slot: %s", e)

    def close(self, connection):
        try:
            connection.close()
        except Exception as e:
            logger.error(
                "error closing receiving connection - ignoring: %s", e)
