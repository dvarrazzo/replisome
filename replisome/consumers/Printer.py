import sys
import json


class Printer(object):
    """
    Print the data received on stdout.
    """
    def __call__(self, msg):
        json.dump(msg, sys.stdout)
        sys.stdout.write('\n')
