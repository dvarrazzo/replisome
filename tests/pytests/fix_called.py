from Queue import Queue, Empty
import pytest

import logging
logger = logging.getLogger('replisome.tests.Called')


@pytest.fixture
def called():
    """Attach a queue to a callable to check if it was called asynchronously.

    Use c = called(object, method_name) to intercept calls to object.method().
    Use c.get() to return the arguments passed to object.method() and the
    return value (it returns a tuple (args, kwargs, rv).  If the method raised
    an exception, c.get() will reraise it. If the method wasn't called c.get()
    will make the test fail.
    """
    cf = CalledFactory()
    yield cf
    for c in cf.called:
        c.restore()


class CalledFactory(object):
    def __init__(self):
        self.called = []

    def __call__(self, obj, attr):
        c = Called(obj, attr, self)
        self.called.append(c)
        return c


class Called(object):
    def __init__(self, obj, attr, request):
        self.obj = obj
        self.attr = attr
        self.request = request
        self._queue = Queue()

        self.orig = getattr(self.obj, self.attr)
        setattr(self.obj, self.attr, self._call)

    def restore(self):
        if hasattr(self, 'orig'):
            setattr(self.obj, self.attr, self.orig)

    def _call(self, *args, **kwargs):
        try:
            rv = self.orig(*args, **kwargs)
        except Exception as e:
            self._queue.put((args, kwargs, e))
            raise
        else:
            self._queue.put((args, kwargs, rv))

        return rv

    def get(self, timeout=1):
        assert timeout
        try:
            rv = self._queue.get(timeout=timeout)
        except Empty:
            pytest.fail("no item received within %s seconds" % timeout)

        if isinstance(rv[2], Exception):
            raise rv[2]
