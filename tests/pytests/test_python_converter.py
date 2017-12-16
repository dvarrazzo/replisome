import pytest
import datetime as dt

from replisome.errors import ConfigError
from replisome.filters.PythonConverter import PythonConverter as PC


def test_no_types():
    with pytest.raises(ConfigError):
        PC.from_config({})


@pytest.mark.parametrize('thing', [{}, None, True, 42, 'hello'])
def test_bad_types(thing):
    with pytest.raises(ConfigError):
        PC.from_config({'types': thing})


def test_date_builtin():
    pc = PC.from_config({'types': ['date']})
    in_ = {"tx": [
        {"op": "I",
        "schema": "public",
        "table": "test",
        "colnames": ["id", "d", "t"],
        "coltypes": ["int4", "date", "text"],
        "values": [1, "2017-05-13", "2017-05-14"]},
    ]}

    r = pc(in_)
    rec = r['tx'][0].pop('record')
    assert r == in_
    assert rec == dict(id=1, d=dt.date(2017, 5, 13), t="2017-05-14")
