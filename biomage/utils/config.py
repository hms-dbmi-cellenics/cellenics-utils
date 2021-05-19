import os
import pathlib

import yaml


def get_config():
    # Read configuration
    config = None
    # this depends on the location of this config file, we use this to avoid depending
    # on the current working dir so that `biomage` cmd can be called from anywhere
    # in the system
    repo_root = pathlib.Path(__file__).parent.parent.parent
    with open(os.path.join(repo_root, "config.yaml")) as config_file:
        config = list(yaml.load_all(config_file, Loader=yaml.SafeLoader))[0]

    return config
