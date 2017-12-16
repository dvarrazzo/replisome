from six import string_types
import datetime as dt

from replisome.config import deep_import
from replisome.errors import ConfigError


class PythonConverter(object):
    """
    Convert data received into Python types.

    The filter can be configured with type mappers to convert PostgreSQL types
    into Python objects.

    The filter will add attributes to all the change objects. Inserts and
    updates will have a ``record`` dict, mapping field names into converted
    values; updates and deletes will have a ``key`` dict too, mapping the key
    field names into the converted values.

    Configuration options:

    - ``types``:

      A list of type mapping specifications, which can be:

      - the name of a builtin converter function, such as ``date``;
      - the name of a builtin converters map, such as ``dates``;
      - a dict from a type name to a builtin converter function, or to an
        user-defined converter function. The latter must be module-qualified,
        e.g. ``some_date_domain: date`` or ``json:
        my_package.converters.my_json_loader``
      - The module-qualified name of an user-defined dict from type names to
        conversion functions, e.g. ``my_package.converters.all_my_types``.

    The conversion functions are functions taking an input value (usually a
    string, unless the object received has already a different representation
    in JSON, such as a bool or a number) which should return the converted
    value. NULLs will not be passed to the functions. The function may
    optionally take keyword arguments:``type_name``, ``field_name``,
    ``table_name``, ``schema_name``, which can be used to customize the
    behavour of the converter -- and I won't insult your intelligence
    explaining you what they will contain.

    Note that unchanged toast fields will not be included in the ``record``.

    Example configuration::

    .. code:: yaml

        filters:
          - class: PythonConverter
            options:
                types:
                  - json
                  - dates
                  - my_json_domain: json
                  - my_type: path.to.my.converter
                  - path.to.my.types.mapping
    """
    def __init__(self, convs):
        self.convs = convs
        self._tables = {}
        self._keys = {}

    @classmethod
    def from_config(cls, config):
        try:
            types = config['types']
        except KeyError:
            raise ConfigError(
                "configuration for %s doesn't contain a 'types' list" %
                cls.__name__)

        if not isinstance(types, list):
            raise ConfigError('types should be a list, got %s' % (types,))

        def resolve(item):
            if '.' not in item:
                item = 'replisome.filters.PythonConverter.%s' % item
            return deep_import(item)

        convs = {}

        for item in types:
            if isinstance(item, string_types):
                item = resolve(item)

            elif isinstance(item, dict):
                item = {k: resolve(v) for k, v in item.items()}

            if not isinstance(item, dict):
                raise ConfigError(
                    "PythonConverter types entries should be types mappings, "
                    "got %s" % item)

            convs.update(item)

        return cls(convs)

    def __call__(self, msg):
        for ch in msg['tx']:
            key = (ch.get('schema'), ch['table'])
            if 'colnames' in ch and 'coltypes' in ch:
                self._tables[key] = (ch['colnames'], ch['coltypes'])
            if 'keynames' in ch and 'keytypes' in ch:
                self._keys[key] = (ch['keynames'], ch['keytypes'])

            if 'values' in ch:
                ch['record'] = self._make_record(ch, self._tables[key], ch['values'])
            if 'oldkey' in ch:
                ch['key'] = self._make_record(ch, self._keys[key], ch['oldkey'])

        return msg

    def _make_record(self, msg, nts, values, _unchanged={}):
        rv = {}
        for n, t, v in zip(nts[0], nts[1], values):
            if v == _unchanged:
                # unchanged toast
                continue
            if v is not None and t in self.convs:
                v = self.convs[t](v)
            rv[n] = v

        return rv


def converter(f):
    """Take a conversion function and make a dictionary out of it.

    The function should be called as the data type to convert.
    """
    return {f.__name__: f}


@converter
def date(s):
    return dt.datetime.strptime(s, '%Y-%m-%d').date()
