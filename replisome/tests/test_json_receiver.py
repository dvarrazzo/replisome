from threading import Thread
from Queue import Queue

from replisome.receivers.json_receiver import JsonReceiver


def test_insert(src_db):
    r = Receiver()
    jr = JsonReceiver(slot=src_db.slot, message_cb=r.receive)
    t = Thread(target=jr.start, args=(src_db.repl_conn,))
    t.start()

    cur = src_db.conn.cursor()
    cur.execute("""
        drop table if exists somedata;

        create table somedata (
            id serial primary key,
            data text,
            float float,
            numeric numeric,
            dt date)
            """)

    cur.execute("insert into somedata default values")

    d = r.received.get(timeout=1)
    assert len(d['change']) == 1
    c = d['change'][0]
    assert c['table'] == 'somedata'
    assert c['schema'] == 'public'
    assert c['colnames'] == 'id data float numeric dt'.split()
    assert c['coltypes'] == 'int4 text float8 numeric date'.split()
    assert c['values'] == [1, None, None, None, None]
    assert 'keynames' not in c
    assert 'keytypes' not in c
    assert 'oldkey' not in c

    cur.execute("""insert into somedata values
        (default, 'hello world', 3.14, 1.01, '2017-01-01')""")

    d = r.received.get(timeout=1)
    assert len(d['change']) == 1
    c = d['change'][0]
    assert c['table'] == 'somedata'
    assert c['schema'] == 'public'
    assert 'colnames' not in c
    assert 'coltypes' not in c
    assert c['values'] == [2, u"hello world", 3.14, 1.01, '2017-01-01']
    assert 'keynames' not in c
    assert 'keytypes' not in c
    assert 'oldkey' not in c

    cur.execute("begin")
    for d in ('t0', 't1', 't2'):
        cur.execute("insert into somedata (data) values (%s)", [d])
    cur.execute("commit")

    d = r.received.get(timeout=1)
    assert len(d['change']) == 3
    for i, c in enumerate(d['change']):
        assert c['table'] == 'somedata'
        assert c['schema'] == 'public'
        assert c['values'][:2] == [3 + i, 't%d' % i]
        assert 'keynames' not in c
        assert 'keytypes' not in c
        assert 'oldkey' not in c

    cur.execute("alter table somedata add newcol text")
    cur.execute("insert into somedata default values")

    d = r.received.get(timeout=1)
    assert len(d['change']) == 1
    c = d['change'][0]
    assert c['table'] == 'somedata'
    assert c['schema'] == 'public'
    assert c['colnames'] == 'id data float numeric dt newcol'.split()
    assert c['coltypes'] == 'int4 text float8 numeric date text'.split()
    assert c['values'] == [6, None, None, None, None, None]
    assert 'keynames' not in c
    assert 'keytypes' not in c
    assert 'oldkey' not in c

    jr.stop()


class Receiver(object):
    def __init__(self):
        self.received = Queue()

    def receive(self, msg):
        self.received.put(msg)
