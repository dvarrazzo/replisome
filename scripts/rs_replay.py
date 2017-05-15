#!/usr/bin/env python

import sys

from replisome.receivers.json_receiver import JsonReceiver
from replisome.consumers.data_updater import DataUpdater

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

    sa = DataUpdater(opt.tgt_dsn)
    rr = JsonReceiver(
        opt.slot, opt.src_dsn, message_cb=sa.process_message)
    rr.options = opt.options
    try:
        rr.start(create=opt.create_slot)
    finally:
        if opt.drop_slot:
            rr.drop_slot()


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
