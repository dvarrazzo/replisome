import pytest
from decimal import Decimal

from replisome.errors import ReplisomeError
from replisome.consumers.DataUpdater import DataUpdater
from replisome.receivers.JsonReceiver import JsonReceiver


def test_insert(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn, skip_missing_columns=True,
                     skip_missing_tables=True)
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


def test_insert_missing_table(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn, skip_missing_columns=True)
    c = called(du, 'process_message')

    rconn = src_db.make_repl_conn()
    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, rconn)

    scur = src_db.conn.cursor()
    tcur = tgt_db.conn.cursor()

    scur.execute("drop table if exists testins")
    scur.execute(
        "create table testins (id serial primary key, data text)")

    tcur.execute("drop table if exists testins")

    scur.execute("insert into testins (data) values ('hello')")
    with pytest.raises(ReplisomeError):
        c.get()

    jr.stop()
    rconn.close()

    tcur.execute("create table testins (id serial primary key, data text)")

    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, src_db.make_repl_conn())
    c.get()

    tcur.execute("select * from testins")
    assert tcur.fetchall() == [(1, 'hello')]


def test_insert_missing_col(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn)
    c = called(du, 'process_message')

    rconn = src_db.make_repl_conn()
    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, rconn)

    scur = src_db.conn.cursor()
    tcur = tgt_db.conn.cursor()

    scur.execute("drop table if exists testins")
    scur.execute(
        "create table testins (id serial primary key, data text, more text)")

    tcur.execute("drop table if exists testins")
    tcur.execute("""
        create table testins (
            id integer primary key,
            data text)
        """)

    scur.execute("insert into testins (data) values ('hello')")
    with pytest.raises(ReplisomeError):
        c.get()

    tcur.execute("select * from testins")
    assert tcur.fetchall() == []

    jr.stop()
    rconn.close()
    tcur.execute("alter table testins add more text")

    du = DataUpdater(tgt_db.conn.dsn)
    c = called(du, 'process_message')

    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, src_db.make_repl_conn())

    c.get()
    tcur.execute("select * from testins")
    assert tcur.fetchall() == [(1, 'hello', None)]


def test_insert_conflict(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn, upsert=True, skip_missing_columns=True)
    c = called(du, 'process_message')

    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, src_db.make_repl_conn())

    scur = src_db.conn.cursor()
    tcur = tgt_db.conn.cursor()

    scur.execute("drop table if exists testins")
    scur.execute("""
        create table testins (
            id serial primary key, data text, foo text, more text)
        """)

    tcur.execute("drop table if exists testins")
    tcur.execute("""
        create table testins (
            id integer primary key, data text, foo text, n int)
        """)

    tcur.execute(
        "insert into testins (id, data, foo, n) values (1, 'foo', 'ouch', 42)")
    scur.execute(
        "insert into testins (data, foo, more) values ('baz', 'qux', 'quux')")

    if tgt_db.conn.server_version >= 90500:
        c.get()
    else:
        with pytest.raises(ReplisomeError):
            c.get()
        pytest.skip()

    tcur.execute("select * from testins")
    rs = tcur.fetchall()
    assert len(rs) == 1
    assert rs[0] == (1, 'baz', 'qux', 42)


def test_insert_conflict_do_nothing(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn, upsert=True, skip_missing_columns=True)
    c = called(du, 'process_message')

    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, src_db.make_repl_conn())

    scur = src_db.conn.cursor()
    tcur = tgt_db.conn.cursor()

    scur.execute("drop table if exists testinsmini")
    scur.execute("create table testinsmini (id serial primary key, data text)")

    tcur.execute("drop table if exists testinsmini")
    tcur.execute("create table testinsmini (id serial primary key, other text)")

    tcur.execute(
        "insert into testinsmini (id, other) values (1, 'foo')")
    scur.execute(
        "insert into testinsmini (data) values ('bar')")

    if tgt_db.conn.server_version >= 90500:
        c.get()
    else:
        with pytest.raises(ReplisomeError):
            c.get()
        pytest.skip()

    tcur.execute("select * from testinsmini")
    rs = tcur.fetchall()
    assert len(rs) == 1
    assert rs[0] == (1, 'foo')


def test_update(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn, skip_missing_columns=True,
                    skip_missing_tables=True)
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


def test_update_missing_table(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn, skip_missing_columns=True)
    c = called(du, 'process_message')

    rconn = src_db.make_repl_conn()
    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, rconn)

    scur = src_db.conn.cursor()
    tcur = tgt_db.conn.cursor()

    scur.execute("drop table if exists testins")
    scur.execute("drop table if exists testins2")
    scur.execute(
        "create table testins (id serial primary key, data text)")

    tcur.execute("drop table if exists testins")
    tcur.execute("drop table if exists testins2")
    tcur.execute("create table testins (id serial primary key, data text)")

    scur.execute("insert into testins (data) values ('hello')")
    c.get()
    scur.execute("alter table testins rename to testins2")
    scur.execute("update testins2 set data = 'world' where id = 1")

    with pytest.raises(ReplisomeError):
        c.get()

    jr.stop()
    rconn.close()

    tcur.execute("alter table testins rename to testins2")

    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, src_db.make_repl_conn())
    c.get()

    tcur.execute("select * from testins2")
    assert tcur.fetchall() == [(1, 'world')]


def test_update_missing_col(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn)
    c = called(du, 'process_message')

    rconn = src_db.make_repl_conn()
    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, rconn)

    scur = src_db.conn.cursor()
    tcur = tgt_db.conn.cursor()

    scur.execute("drop table if exists testup")
    scur.execute(
        "create table testup (id serial primary key, data text)")

    tcur.execute("drop table if exists testup")
    tcur.execute("""
        create table testup (
            id integer primary key,
            data text)
        """)

    scur.execute("insert into testup (data) values ('hello')")
    scur.execute("insert into testup (data) values ('world')")

    scur.execute("alter table testup add more text")
    scur.execute("update testup set more = 'mama' where id = 2")

    for i in range(2):
        c.get()

    with pytest.raises(ReplisomeError):
        c.get()

    tcur.execute("select id, data from testup order by id")
    rs = tcur.fetchall()
    assert rs == [(1, 'hello'), (2, 'world')]

    jr.stop()
    rconn.close()

    tcur.execute("alter table testup add more text")

    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, src_db.make_repl_conn())
    c.get()

    tcur.execute("select id, data, more from testup order by id")
    rs = tcur.fetchall()
    assert rs == [(1, 'hello', None), (2, 'world', 'mama')]


def test_delete(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn, skip_missing_columns=True,
                    skip_missing_tables=True)
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


def test_delete_missing_table(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn, skip_missing_columns=True)
    c = called(du, 'process_message')

    rconn = src_db.make_repl_conn()
    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, rconn)

    scur = src_db.conn.cursor()
    tcur = tgt_db.conn.cursor()

    scur.execute("drop table if exists testins")
    scur.execute("drop table if exists testins2")
    scur.execute(
        "create table testins (id serial primary key, data text)")

    tcur.execute("drop table if exists testins")
    tcur.execute("drop table if exists testins2")
    tcur.execute("create table testins (id serial primary key, data text)")

    scur.execute("insert into testins (data) values ('hello'), ('world')")
    c.get()

    scur.execute("alter table testins rename to testins2")
    scur.execute("delete from testins2 where id = 1")

    with pytest.raises(ReplisomeError):
        c.get()

    jr.stop()
    rconn.close()

    tcur.execute("alter table testins rename to testins2")

    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, src_db.make_repl_conn())
    c.get()

    tcur.execute("select * from testins2")
    assert tcur.fetchall() == [(2, 'world')]


def test_toast(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn, skip_missing_columns=True)
    c = called(du, 'process_message')

    rconn = src_db.make_repl_conn()
    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, rconn)

    scur = src_db.conn.cursor()
    tcur = tgt_db.conn.cursor()

    scur.execute("drop table if exists xpto")
    scur.execute("""
        create table xpto (
            id serial primary key,
            toast1 text,
            other float8,
            toast2 text)
        """)

    tcur.execute("drop table if exists xpto")
    tcur.execute("""
        create table xpto (
            id integer primary key,
            toast1 text,
            other float8,
            toast2 text)
        """)

    # uncompressed external toast data
    scur.execute("""
        insert into xpto (toast1, toast2)
        select string_agg(g.i::text, ''), string_agg((g.i*2)::text, '')
        from generate_series(1, 2000) g(i)
        """)
    c.get()
    tcur.execute("select * from xpto where id = 1")
    r1 = tcur.fetchone()
    assert r1
    assert len(r1[1]) > 1000
    assert len(r1[3]) > 1000

    # compressed external toast data
    scur.execute("""
        insert into xpto (toast2)
        select repeat(string_agg(to_char(g.i, 'fm0000'), ''), 50)
        from generate_series(1, 500) g(i)
        """)
    c.get()
    tcur.execute("select * from xpto where id = 2")
    r2 = tcur.fetchone()
    assert r2
    assert r2[1] is None
    assert len(r2[3]) > 1000

    # update of existing column
    scur.execute("""
        update xpto set toast1 = (select string_agg(g.i::text, '')
        from generate_series(1, 2000) g(i)) where id = 1
        """)
    c.get()
    tcur.execute("select * from xpto where id = 1")
    r11 = tcur.fetchone()
    assert len(r11[1]) > 1000
    assert r11[1] == r1[1]
    assert r11[3] == r1[3]

    scur.execute("update xpto set other = 123.456 where id = 1")
    c.get()
    tcur.execute("select * from xpto where id = 1")
    r12 = tcur.fetchone()
    assert len(r12[1]) > 1000
    assert len(r12[3]) > 1000
    assert r11[1] == r12[1]
    assert r12[3] == r12[3]

    scur.execute("delete from xpto where id = 1")
    c.get()
    tcur.execute("select * from xpto where id = 1")
    assert not tcur.fetchone()


def test_numeric(src_db, tgt_db, called):
    du = DataUpdater(tgt_db.conn.dsn)
    c = called(du, 'process_message')

    rconn = src_db.make_repl_conn()
    jr = JsonReceiver(slot=src_db.slot, message_cb=du.process_message)
    src_db.thread_receive(jr, rconn)

    scur = src_db.conn.cursor()
    tcur = tgt_db.conn.cursor()

    for _c in [scur, tcur]:
        _c.execute("drop table if exists num")
        _c.execute("""
            create table num (
                id integer primary key,
                num numeric)
            """)

    scur.execute("""
        insert into num values
        (1, 1.000000000000000000000000000000000000000000000000000000000000001)
        """)
    scur.execute("select num from num where id = 1")
    sn = scur.fetchone()[0]
    assert isinstance(sn, Decimal)
    assert len(str(sn)) > 20

    c.get()
    tcur.execute("select num from num where id = 1")
    tn = tcur.fetchone()[0]
    assert tn == sn
