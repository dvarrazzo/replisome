import yaml
import pytest

from replisome import config

TEST_CONFIG = """
receiver:
    class: JsonReceiver
    dsn: %(src_dsn)s
    slot: %(slot)s
    plugin: replisome
    options:
        pretty_print: false
        includes:
          - schema: myapp
            tables: '^contract(_expired_\d{6})?$'
            where: "seller in ('alice', 'bob')"
          - schema: myapp
            table: account
            skip_columns: [password]

consumer:
    class: DataUpdater
    options:
        dsn: %(tgt_dsn)s
"""


SRC_SCHEMA = """
create schema if not exists myapp;

drop table if exists myapp.useless;
drop table if exists myapp.contract cascade;
drop table if exists myapp.account cascade;

create table myapp.account (
    id serial primary key,
    username text,
    password text
);

create table myapp.contract (
    id serial primary key,
    account int not null references myapp.account,
    seller text not null,
    date date not null,
    data text
);

create table myapp.useless (
    id serial primary key,
    data text
);
"""


def to_yaml(s):
    """Convert a python string into a yaml string"""
    return yaml.dump(s, default_style='"').rstrip()


@pytest.fixture
def configfile(tmpdir, src_db, tgt_db):
    args = dict(
        src_dsn=to_yaml(src_db.dsn), tgt_dsn=to_yaml(tgt_db.dsn),
        slot=to_yaml(src_db.slot))
    data = TEST_CONFIG % args
    f = tmpdir.join('test.yaml')
    f.write(data)
    return str(f)


def test_pipeline(configfile, src_db, tgt_db, called):
    scur = src_db.conn.cursor()
    scur.execute(SRC_SCHEMA)
    tcur = tgt_db.conn.cursor()
    tcur.execute(SRC_SCHEMA)

    conf = config.parse_yaml(configfile)
    pl = config.make_pipeline(conf)
    c = called(pl.consumer, 'process_message')

    src_db.thread_run(pl.start, pl.stop)

    scur.execute("""
        insert into myapp.account (username, password) values
            ('acc1', 'secret1'),
            ('acc2', 'secret2')
        """)

    scur.execute(
        "insert into myapp.useless default values")

    scur.execute("""
        insert into myapp.contract (account, seller, date)
        values (1, 'alice', '2017-01-01')
        """)
    scur.execute("""
        insert into myapp.contract (account, seller, date) values
            (2, 'charlie', '2017-02-01'),
            (1, 'bob', '2017-03-01')
        """)

    for i in range(3):
        c.get()

    tcur.execute("select username, password from myapp.account order by 1")
    assert tcur.fetchall() == [('acc1', None), ('acc2', None)]

    tcur.execute("select id from myapp.useless")
    assert tcur.fetchall() == []

    tcur.execute("select username, password from myapp.account order by 1")
    assert tcur.fetchall() == [('acc1', None), ('acc2', None)]

    tcur.execute("select id, seller from myapp.contract order by 1")
    assert tcur.fetchall() == [(1, 'alice'), (3, 'bob')]
