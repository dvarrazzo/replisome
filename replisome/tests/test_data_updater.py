from replisome.consumers.DataUpdater import DataUpdater
from replisome.receivers.JsonReceiver import JsonReceiver


def test_insert(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn)
    c = called(du, 'process_message')

    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
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
    c.get()
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

    c.get()
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
    c.get()
    scur.execute("insert into testins default values")
    c.get()

    tcur.execute("select max(id) from testins")
    assert tcur.fetchone()[0] == 4


def test_insert_conflict(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn, upsert=True)
    c = called(du, 'process_message')

    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, src_db.make_repl_conn())

    scur = src_db.conn.cursor()
    tcur = tgt_db.conn.cursor()

    scur.execute("drop table if exists testins")
    scur.execute(
        "create table testins (id serial primary key, data text, foo text, more text)")

    tcur.execute("drop table if exists testins")
    tcur.execute(
        "create table testins ( id integer primary key, data text, foo text, n int)")

    tcur.execute(
        "insert into testins (id, data, foo, n) values (1, 'foo', 'ouch', 42)")
    scur.execute(
        "insert into testins (data, foo, more) values ('baz', 'qux', 'quux')")
    c.get()

    tcur.execute("select * from testins")
    rs = tcur.fetchall()
    assert len(rs) == 1
    assert rs[0] == (1, 'baz', 'qux', 42)


def test_update(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn)
    c = called(du, 'process_message')

    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
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
        c.get()

    tcur.execute("select id, data from testup order by id")
    rs = tcur.fetchall()
    assert rs == [(1, 'hello'), (2, 'mama')]

    # The key can change too
    scur.execute("update testup set id = 22 where id = 2")
    c.get()

    tcur.execute("select id, data from testup order by id")
    rs = tcur.fetchall()
    assert rs == [(1, 'hello'), (22, 'mama')]

    # Missing tables are ignored
    scur.execute("drop table if exists notable")
    scur.execute(
        "create table notable (id serial primary key)")

    scur.execute("insert into notable default values")
    c.get()
    scur.execute("update notable set id = 2 where id = 1")
    c.get()
    scur.execute("insert into testup default values")
    c.get()

    tcur.execute("select id from testup where id = 3")
    assert tcur.fetchone()[0] == 3


def test_delete(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn)
    c = called(du, 'process_message')

    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, src_db.make_repl_conn())

    scur = src_db.conn.cursor()
    tcur = tgt_db.conn.cursor()

    scur.execute("drop table if exists testdel")
    scur.execute(
        "create table testdel (id serial primary key, data text)")

    tcur.execute("drop table if exists testdel")
    tcur.execute(
        "create table testdel (id serial primary key, data text)")

    scur.execute("insert into testdel (data) values ('hello')")
    scur.execute("insert into testdel (data) values ('world')")
    scur.execute("delete from testdel where id = 2")
    scur.execute("insert into testdel (data) values ('mama')")

    for i in range(4):
        c.get()

    tcur.execute("select id, data from testdel order by id")
    rs = tcur.fetchall()
    assert rs == [(1, 'hello'), (3, 'mama')]

    # Missing tables are ignored
    scur.execute("drop table if exists notable")
    scur.execute(
        "create table notable (id serial primary key)")

    scur.execute("insert into notable default values")
    c.get()
    scur.execute("delete from notable where id = 1")
    c.get()
    scur.execute("insert into testdel default values")
    c.get()

    tcur.execute("select id from testdel where id = 4")
    assert tcur.fetchone()[0] == 4
