from Queue import Queue

from replisome.consumers.data_updater import DataUpdater
from replisome.receivers.json_receiver import JsonReceiver


def test_insert(src_db, tgt_db):
    du = DataUpdater(tgt_db.conn.dsn)
    q = Queue()     # For synchronization

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
            id integer primary key,
            data text,
            ts timestamptz not null default now(),
            clock timestamptz not null default clock_timestamp())
        """)

    scur.execute("insert into testins (data) values ('hello')")
    q.get(timeout=1)
    tcur.execute("select * from testins")
    rs = tcur.fetchall()
    assert len(rs) == 1
    r = rs[0]
    assert r[0] == 1
    assert r[1] == 'hello'

    # Test records are inserted in the same transaction
    scur.execute("begin")
    scur.execute("insert into testins default values")
    scur.execute("insert into testins default values")
    scur.execute("commit")

    q.get(timeout=1)
    tcur.execute("select * from testins where id > 1 order by id")
    rs = tcur.fetchall()
    assert len(rs) == 2

    assert rs[0][0] == 2
    assert rs[1][0] == 3

    assert rs[0][2] == rs[1][2]
    assert rs[0][3] < rs[1][3]

    # Missing tables are ignored
    scur.execute("drop table if exists notable")
    scur.execute(
        "create table notable (id serial primary key)")

    scur.execute("insert into notable default values")
    q.get(timeout=1)
    scur.execute("insert into testins default values")
    q.get(timeout=1)

    tcur.execute("select max(id) from testins")
    assert tcur.fetchone()[0] == 4


def test_update(src_db, tgt_db):
    du = DataUpdater(tgt_db.conn.dsn)
    q = Queue()     # For synchronization

    def cb(msg):
        du.process_message(msg)
        q.put(None)

    jr = JsonReceiver(slot=src_db.slot, message_cb=cb)
    src_db.thread_receive(jr, src_db.make_repl_conn())

    scur = src_db.conn.cursor()
    tcur = tgt_db.conn.cursor()

    scur.execute("drop table if exists testup")
    scur.execute(
        "create table testup (id serial primary key, data text, more text)")

    tcur.execute("drop table if exists testup")
    tcur.execute("""
        create table testup (
            id integer primary key,
            data text,
            ts timestamptz not null default now(),
            clock timestamptz not null default clock_timestamp())
        """)

    scur.execute("insert into testup (data) values ('hello')")
    scur.execute("insert into testup (data) values ('world')")
    scur.execute("update testup set data = 'mama' where id = 2")

    for i in range(3):
        q.get(timeout=1)

    tcur.execute("select id, data from testup order by id")
    rs = tcur.fetchall()
    assert rs == [(1, 'hello'), (2, 'mama')]

    # The key can change too
    scur.execute("update testup set id = 22 where id = 2")
    q.get(timeout=1)

    tcur.execute("select id, data from testup order by id")
    rs = tcur.fetchall()
    assert rs == [(1, 'hello'), (22, 'mama')]

    # Missing tables are ignored
    scur.execute("drop table if exists notable")
    scur.execute(
        "create table notable (id serial primary key)")

    scur.execute("insert into notable default values")
    q.get(timeout=1)
    scur.execute("update notable set id = 2 where id = 1")
    q.get(timeout=1)
    scur.execute("insert into testup default values")
    q.get(timeout=1)

    tcur.execute("select id from testup where id = 3")
    assert tcur.fetchone()[0] == 3
