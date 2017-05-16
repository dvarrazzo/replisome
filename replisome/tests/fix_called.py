from Queue import Queue, Empty
import pytest

import logging
logger = logging.getLogger('replisome.tests.Called')


@pytest.fixture
def called():
    """Attach a queue to a callable to check if it is called asynchronously."""
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

        logger.info("instrumenting method %s of %s", self.attr, self.obj)
        self.orig = getattr(self.obj, self.attr)
        setattr(self.obj, self.attr, self._call)

    def restore(self):
        if hasattr(self, 'orig'):
            logger.info("restoring method %s of %s", self.attr, self.obj)
            setattr(self.obj, self.attr, self.orig)

    def _call(self, *args, **kwargs):
        logger.info("calling method %s of %s", self.attr, self.obj)
        rv = self.orig(*args, **kwargs)
        logger.info("called method %s of %s", self.attr, self.obj)
        self._queue.put((args, kwargs, rv))
        return rv

    def get(self, timeout=1):
        assert timeout
        try:
            return self._queue.get(timeout=timeout)
        except Empty:
            pytest.fail("no item received within %s seconds" % timeout)
