#!/usr/bin/env python
"""Receive data from a database, do something with it.
"""

import sys

from .errors import ReplisomeError
from .config import parse_yaml, make_pipeline

import logging
logger = logging.getLogger('replisome')


def main():
    opt = parse_cmdline()
    logging.basicConfig(
        level=opt.loglevel,
        format='%(asctime)s %(levelname)s %(message)s')

    if opt.configfile:
        conf = parse_yaml(opt.configfile)
    else:
        conf = {
            'receiver': {'class': 'JsonReceiver'},
            'consumer': {'class': 'Printer'}}

    pl = make_pipeline(conf, dsn=opt.dsn, slot=opt.slot)
    pl.start()


def parse_cmdline():
    from argparse import ArgumentParser
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('configfile', nargs='?',
        help="configuration file to parse; if not specified print on stderr")

    parser.add_argument('--dsn',
        help="database to read from (override config file)")
    parser.add_argument('--slot',
        help="the replication slot to connect to (override config file)")

    g = parser.add_mutually_exclusive_group()
    g.add_argument('-v', '--verbose', dest='loglevel',
        action='store_const', const=logging.DEBUG, default=logging.INFO,
        help="print debugging information to stderr")
    g.add_argument('-q', '--quiet', dest='loglevel',
        action='store_const', const=logging.WARN,
        help="minimal output on stderr")

    opt = parser.parse_args()

    return opt


def entry_point():
    try:
        sys.exit(main())

    except ReplisomeError as e:
        logger.error("%s", e)
        sys.exit(1)

    except Exception:
        logger.exception("unexpected error")
        sys.exit(1)

    except KeyboardInterrupt:
        logger.info("user interrupt")
        sys.exit(1)


if __name__ == '__main__':
    entry_point()
