from distutils.version import StrictVersion

from .errors import ReplisomeError

VERSION = '0.1.0'

# Complain if the server doesn't speak a version between these two
SERVER_MIN_VER = StrictVersion('0.1')
SERVER_MAX_VER = StrictVersion('0.2')


def check_version(ver):
    if not SERVER_MIN_VER <= ver < SERVER_MAX_VER:
        raise ReplisomeError(
            'this client supports server versions between %s and %s; '
            'the server has %s installed' %
            (SERVER_MIN_VER, SERVER_MAX_VER, ver))
