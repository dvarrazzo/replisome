import os
import time
import pytest
import subprocess as sp
from threading import Thread

from .test_config import SRC_SCHEMA


def test_version():
    cmdline = [
        os.path.join(os.path.dirname(__file__), '../../scripts/replisome'),
        '--version']

    from replisome.version import VERSION
    rv = sp.check_output(cmdline, stderr=sp.STDOUT)
    assert rv == 'replisome %s\n' % VERSION


@pytest.mark.xfail
def test_minimal_cmdline(src_db):
    scur = src_db.conn.cursor()
    scur.execute(SRC_SCHEMA)

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

    # Wat? just WAT? The script works manually but produces no stdout.
    # Even adding a random print there, it works from command line but not
    # from this test???
    cmdline = [
        os.path.join(os.path.dirname(__file__), '../../scripts/replisome'),
        '--dsn', src_db.dsn, '--slot', src_db.slot]

    p = sp.Popen(cmdline, stdout=sp.PIPE, stderr=sp.PIPE)

    def killer():
        time.sleep(1)
        p.terminate()

    t = Thread(target=killer)
    t.start()
    out, err = p.communicate()
    assert err
    for l in err.splitlines():
        assert 'INFO' in l

    assert out
