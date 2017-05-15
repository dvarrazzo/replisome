from Queue import Queue

from replisome.consumers.data_updater import DataUpdater
from replisome.receivers.json_receiver import JsonReceiver


def test_insert(src_db, tgt_db):
    du = DataUpdater(tgt_db.conn.dsn)
    q = Queue()

    def cb(msg):
        du.process_message(msg)
        q.put(None)

    jr = JsonReceiver(slot=src_db.slot, message_cb=cb)
    src_db.thread_receive(jr, src_db.make_repl_conn())

    scur = src_db.conn.cursor()
    tcur = tgt_db.conn.cursor()

    scur.execute("drop table if exists testins")
    scur.execute(
        "create table testins (id serial primary key, data text, more text)")

    tcur.execute("drop table if exists testins")
    tcur.execute("""
        create table testins (
            id serial primary key,
            data text,
            ts timestamptz not null default now(),
            clock timestamptz not null default clock_timestamp())
        """)

    scur.execute("insert into testins (data) values ('hello')")
    q.get()
    tcur.execute("select * from testins")
    rs = tcur.fetchall()
    assert len(rs) == 1
    r = rs[0]
    assert r[0] == 1
    assert r[1] == 'hello'

    scur.execute("begin")
    scur.execute("insert into testins default values")
    scur.execute("insert into testins default values")
    scur.execute("commit")

    q.get()
    tcur.execute("select * from testins where id > 1 order by id")
    rs = tcur.fetchall()
    assert len(rs) == 2

    assert rs[0][0] == 2
    assert rs[1][0] == 3

    assert rs[0][2] == rs[1][2]
    assert rs[0][3] < rs[1][3]
