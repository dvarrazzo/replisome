#!/usr/bin/env python

import sys

from replisome.receivers.JsonReceiver import JsonReceiver
from replisome.consumers.DataUpdater import DataUpdater

import logging
logging.basicConfig(
    level=logging.INFO, stream=sys.stderr,
    format="%(asctime)s %(levelname)s %(message)s")

logger = logging.getLogger('replisome')


class ScriptError(Exception):
    """Controlled exception raised by the script."""


def main():
    opt = parse_cmdline()

    if opt.verbose:
        logger.setLevel(logging.DEBUG)

    du = DataUpdater(opt.tgt_dsn)
    jr = JsonReceiver(
        slot=opt.slot, dsn=opt.src_dsn, message_cb=du.process_message,
        options=opt.options)
    cnn = jr.create_connection()
    try:
        jr.start(cnn, create=opt.create_slot)
    finally:
        cnn.close()
        if opt.drop_slot:
            jr.drop_slot()


def parse_cmdline():
    from argparse import ArgumentParser
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        'slot',
        help="slot name to use to receive data")
    parser.add_argument(
        'src_dsn',
        help="connection string to connect to and read data")
    parser.add_argument(
        'tgt_dsn',
        help="connection string to connect to and write data")
    parser.add_argument(
        '-o', metavar="KEY=VALUE", action='append', dest='options',
        default=[], help="pass some option to the decode plugin")
    parser.add_argument(
        '--create-slot', action='store_true', default=False,
        help="create the slot before streaming")
    parser.add_argument(
        '--drop-slot', action='store_true', default=False,
        help="drop the slot after streaming")
    parser.add_argument(
        '--verbose', action='store_true',
        help="print more log messages")

    opt = parser.parse_args()

    for i, o in enumerate(opt.options):
        try:
            k, v = o.split('=', 1)
        except ValueError:
            parser.error("bad option: %s" % o)
        else:
            opt.options[i] = (k, v)

    return opt


if __name__ == '__main__':
    try:
        sys.exit(main())

    except ScriptError, e:
        logger.error("%s", e)
        sys.exit(1)

    except Exception:
        logger.exception("unexpected error")
        sys.exit(1)

    except KeyboardInterrupt:
        logger.info("user interrupt")
        sys.exit(1)
