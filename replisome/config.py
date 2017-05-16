from .errors import ConfigError
from .pipeline import Pipeline

import sys
import yaml


def parse_yaml(filename):
    if filename == '-':
        return parse_yaml_file(sys.stdin)
    else:
        try:
            with open(filename) as f:
                return parse_yaml_file(f)
        except IOError as e:
            raise ConfigError(e)


def parse_yaml_file(f):
    try:
        return yaml.load(f)
    except Exception as e:
        raise ConfigError("bad config file: %s" % e)


def make_pipeline(config):
    pl = Pipeline()
    pl.receiver = make_receiver(config.get('receiver'))
    pl.consumer = make_consumer(config.get('consumer'))
    for f in make_filters(config.get('filters')):
        pl.filters.append(f)
    return pl


def make_receiver(config):
    try:
        obj = make_object(config, package='replisome.receivers')
    except ConfigError as e:
        raise ConfigError("bad receiver configuration: %s" % e)

    try:
        obj.dsn = config.pop('dsn')
    except KeyError:
        raise ConfigError("no dsn defined in receiver")

    try:
        obj.slot = config.pop('slot')
    except KeyError:
        raise ConfigError("no slot defined in receiver")

    obj.plugin = config.pop('plugin', 'replisome')

    if config:
        raise ConfigError(
            "unknown receiver configuration entries: %s" %
            ', '.join(sorted(config)))

    return obj


def make_consumer(config):
    try:
        obj = make_object(config, package='replisome.consumers')
    except ConfigError as e:
        raise ConfigError("bad consumer configuration: %s" % e)
    return obj


def make_filters(config):
    if not config:
        return
    if not isinstance(config, list):
        raise ConfigError("filters configuration must be a sequence")

    for f in config:
        yield make_filter(f)


def make_filter(config):
    try:
        obj = make_object(config, package='replisome.filters')
    except ConfigError as e:
        raise ConfigError("bad filter configuration: %s" % e)
    return obj


def deep_import(name):
    pkgname, objname = name.rsplit('.', 1)
    m = __import__(pkgname, fromlist=[objname])
    return getattr(m, objname)


def make_object(config, package=None):
    if not isinstance(config, dict):
        raise ConfigError("config should be an object")

    try:
        cls = config.pop('class')
    except KeyError:
        raise ConfigError("no class specified")

    try:
        options = config.pop('options')
    except KeyError:
        raise ConfigError("no options specified")

    if not isinstance(options, dict):
        raise ConfigError("options should be an object")

    if package and '.' not in cls:
        cls = "%s.%s.%s" % (package, cls, cls)

    try:
        cls = deep_import(cls)
    except ImportError as e:
        raise ConfigError("error importing class: %s: %s" % (cls, e))

    if not isinstance(cls, type):
        raise ConfigError("not a type: %s" % (cls,))

    if hasattr(cls, 'from_config'):
        return cls.from_config(options)
    else:
        return cls(**options)
