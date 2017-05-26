import os
from threading import Thread

import pytest
import psycopg2
from psycopg2.extras import LogicalReplicationConnection, wait_select

from replisome.errors import ReplisomeError


@pytest.fixture
def src_db():
    "Return the source database to connect for testing"
    dsn = os.environ.get(
        "RS_TEST_SRC_DSN",
        'host=localhost dbname=rs_src_test')
    db = TestDatabase(dsn)
    db.drop_slot()
    db.create_slot(if_not_exists=True)

    # Create the extension to make the version number available
    cnn = db.make_conn()
    cur = cnn.cursor()
    cur.execute('drop extension if exists replisome')
    cur.execute('create extension replisome')
    cnn.close()

    yield db
    db.teardown()


@pytest.fixture
def tgt_db():
    "Return the target database to connect for testing"
    dsn = os.environ.get(
        "RS_TEST_TGT_DSN",
        'host=localhost dbname=rs_tgt_test')
    db = TestDatabase(dsn)
    yield db
    db.teardown()


class TestDatabase(object):
    """
    The representation of a database used for testing.

    The database can be the sender of the receiver: a few methods may make
    sense only in one case.

    The object manages one replication slot.
    """
    def __init__(self, dsn):
        self.dsn = dsn
        self.slot = 'rs_test'
        self.plugin = 'replisome'

        self._repl_conn = self._conn = None
        self._conns = []
        self._stop_callbacks = []
        self._slot_created = False

    @property
    def conn(self):
        """
        A connection to the database.

        The object is always the same for the object lifetime.
        """
        if not self._conn:
            self._conn = self.make_conn()
        return self._conn

    @property
    def repl_conn(self):
        """
        A replication connection to the database.

        The object is always the same for the object lifetime.
        """
        if not self._repl_conn:
            self._repl_conn = self.make_repl_conn()
        return self._repl_conn

    def at_exit(self, cb):
        self._stop_callbacks.append(cb)

    def teardown(self):
        """
        Close the database connections and stop any receiving thread.

        Invoked at the end of the tests.
        """
        for cb in self._stop_callbacks:
            cb()

        for cnn in self._conns:
            cnn.close()

        if self._slot_created:
            self.drop_slot()

    def make_conn(self, autocommit=True, **kwargs):
        """Create a new connection to the test database.

        The connection is autocommit, and will be closed on teardown().
        """
        cnn = psycopg2.connect(self.dsn, **kwargs)
        cnn.autocommit = autocommit
        self._conns.append(cnn)
        return cnn

    def make_repl_conn(self, **kwargs):
        """Create a new replication connection to the test database.

        The connection is asynchronous, and will be closed on teardown().
        """
        cnn = psycopg2.connect(
            self.dsn, connection_factory=LogicalReplicationConnection,
            async_=True, **kwargs)
        wait_select(cnn)
        self._conns.append(cnn)
        return cnn

    def drop_slot(self):
        """Delete the replication slot with the current name if exists."""
        with self.make_conn() as cnn:
            cur = cnn.cursor()
            cur.execute("""
                select pg_drop_replication_slot(slot_name)
                from pg_replication_slots
                where slot_name = %s
                """, (self.slot,))

        # Closing explicitly as the function can be called in teardown, after
        # other connections (maybe using the slot) have been closed.
        cnn.close()

    def create_slot(self, if_not_exists=False):
        """Create the replication slot.

        :param if_not_exists:
            If True and the slot exists don't do anything.
            If False create the slot (if it exists, die of error)
        """
        with self.make_conn() as cnn:
            cur = cnn.cursor()
            if if_not_exists:
                cur.execute("""
                    select 1 from pg_replication_slots where slot_name = %s
                    """, (self.slot,))
                if cur.fetchone():
                    return

            cur.execute(
                "select pg_create_logical_replication_slot(%s, %s)",
                [self.slot, self.plugin])

            self._slot_created = True

    def thread_receive(self, receiver, connection, target=None):
        """
        Run the receiver loop of a receiver in a thread.

        Stop the receiver at the end of the test.
        """
        def thread_receive_(cnn):
            try:
                (target or receiver.start)(cnn)
            except ReplisomeError:
                pass

        self.thread_run(
            target=thread_receive_, at_exit=receiver.stop,
            args=(connection,))

    def thread_run(self, target, at_exit=None, args=()):
        """Call a function in a thread. Call another function on stop."""
        if at_exit:
            self.at_exit(at_exit)

        t = Thread(target=target, args=args)
        t.start()
