
import getty
import sqlalchemy as sa
from meta_db import MetaTableBase

class _config(MetaTableBase):
    __tablename__ = 'config'

    # mapped attributes
    name = sa.Column(sa.String, primary_key = True)
    value = sa.Column(sa.String, nullable = False)

    def __init__(self, name, value):
        self.name = name
        self.value = value

# Factory which maps a configuration
#  name to an injectable class
__cfg_classes = {}
def Config(config_name):

    global __cfg_classes
    if config_name in __cfg_classes:
        return __cfg_classes[config_name]

    class __inj_config_cls(object):
        def __init__(self):
            raise RuntimeError("Configuration %s is not defined"\
                % config_name)

    __cfg_classes[config_name] = __inj_config_cls
    return __inj_config_cls

def make_configuration_bindings(injector, session):

    for conf in session.query(_config):

        # Don't replace an existing override binding
        if (Config(conf.name), None) in injector._bindings:
            continue

        injector.bind_instance(Config(conf.name), conf.value)

