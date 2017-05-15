import os
import pytest
import psycopg2
from psycopg2.extras import LogicalReplicationConnection, wait_select


@pytest.fixture
def src_db():
    dsn = os.environ.get(
        "RS_TEST_SRC_DSN",
        'host=localhost dbname=rs_src_test')
    db = TestDatabase(dsn)
    yield db
    db.teardown()


class TestDatabase(object):
    def __init__(self, dsn):
        self.dsn = dsn
        self.slot = 'rs_test'
        self.plugin = 'replisome'

        self._conn = None
        self._repl_conn = None

    @property
    def conn(self):
        if not self._conn:
            self._conn = self.make_conn()
        return self._conn

    @property
    def repl_conn(self):
        if not self._repl_conn:
            self._repl_conn = self.make_repl_conn()
        return self._repl_conn

    def teardown(self):
        if self._repl_conn:
            self._repl_conn.close()

    def make_conn(self, autocommit=True, **kwargs):
        cnn = psycopg2.connect(self.dsn, **kwargs)
        cnn.autocommit = autocommit
        return cnn

    def make_repl_conn(self, **kwargs):
        self.create_slot(if_not_exists=True)
        cnn = psycopg2.connect(
            self.dsn, connection_factory=LogicalReplicationConnection,
            async_=True, **kwargs)
        wait_select(cnn)
        return cnn

    def create_slot(self, if_not_exists=False):
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
