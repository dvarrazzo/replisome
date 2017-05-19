import re

from replisome.errors import ConfigError

import logging
logger = logging.getLogger('replisome.TableRenamer')


class TableRenamer(object):
    """
    Change table or schema names in received tables
    """
    def __init__(self, from_table=None, from_tables=None,
                 from_schema=None, from_schemas=None,
                 to_table=None, to_schema=None):
        if from_table and from_tables:
            raise ConfigError("can't specify both from_table and from_tables")
        if from_schema and from_schemas:
            raise ConfigError("can't specify both from_table and from_tables")
        if not (from_table or from_tables or from_schema or from_schemas):
            raise ConfigError(
                "you must specify at least a source table or schema")
        if not (from_table or from_tables or from_schema or from_schemas):
            raise ConfigError(
                "you must specify at least a destination table or schema")

        self.from_table = from_table
        self.from_tables = re.compile(from_tables) if from_tables else None
        self.from_schema = from_schema
        self.from_schemas = re.compile(from_schemas) if from_schemas else None
        self.to_table = to_table
        self.to_schema = to_schema

    def __call__(self, msg):
        return self.process_message(msg)

    def process_message(self, msg):
        for ch in msg['tx']:
            if self.from_table is not None:
                if self.from_table != ch['table']:
                    continue
            if self.from_tables is not None:
                if not self.from_tables.match(ch['table']):
                    continue
            if 'schema' in ch:
                if self.from_schema is not None:
                    if self.from_schema != ch['schema']:
                        continue
                if self.from_schemas is not None:
                    if not self.from_schemas.match(ch['schema']):
                        continue

            if self.to_table is not None:
                ch['table'] = self.to_table
            if self.to_schema is not None and 'schema' in ch:
                ch['schema'] = self.to_schema

        return msg
