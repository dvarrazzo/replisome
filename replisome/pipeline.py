import psycopg2
from distutils.version import StrictVersion

from .errors import ReplisomeError


# Complain if the server doesn't speak a version between these two
MIN_VER = StrictVersion('0.1')
MAX_VER = StrictVersion('0.2')


class Pipeline(object):
    """A chain of operations on a stream of changes"""
    NOT_STARTED = 'NOT_STARTED'
    RUNNING = 'RUNNING'
    STOPPED = 'STOPPED'

    def __init__(self):
        self.receiver = None
        self.filters = []
        self.consumer = None
        self.state = self.NOT_STARTED

    def start(self):
        if self.state != self.NOT_STARTED:
            raise ValueError("can't start pipeline in state %s" % self.state)

        if not self.receiver:
            raise ValueError("can't start: no receiver")

        if not self.consumer:
            raise ValueError("can't start: no consumer")

        self.verify_version()
        self.receiver.message_cb = self.process_message
        cnn = self.receiver.create_connection()

        self.state = self.RUNNING
        self.receiver.start(cnn)

    def stop(self):
        if self.state != self.RUNNING:
            raise ValueError("can't stop pipeline in state %s" % self.state)

        self.receiver.stop()
        self.state = self.STOPPED

    def process_message(self, msg):
        for f in self.filters:
            msg = f(msg)
            if msg is None:
                return

        self.consumer(msg)

    def verify_version(self):
        cnn = psycopg2.connect(self.receiver.dsn)
        cur = cnn.cursor()
        try:
            cur.execute('select replisome_version()')
        except psycopg2.ProgrammingError as e:
            if e.pgcode == '42883':     # function not found
                raise ReplisomeError("function replisome_version() not found")
            else:
                raise
        else:
            version = cur.fetchone()[0]
        finally:
            cnn.rollback()
            cnn.close()

        if not MIN_VER <= version < MAX_VER:
            raise ReplisomeError(
                'this client supports server versions between %s and %s; '
                'the server has %s installed' % (MIN_VER, MAX_VER, version))
