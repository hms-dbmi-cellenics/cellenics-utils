import pkgutil

import yaml


def get_config():
    # Read configuration
    # this depends on the location of this config file, we use this to avoid depending
    # on the current working dir so that `biomage` cmd can be called from anywhere
    # in the system
    config = pkgutil.get_data('biomage', 'config.yaml').decode('utf-8')
    return yaml.load(config, Loader=yaml.SafeLoader)
