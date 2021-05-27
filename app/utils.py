import os

import yaml


def get_config(key: str) -> dict:
    current_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(current_dir, 'config.yaml')
    if not os.path.exists(config_path):
        return dict()
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    if key in config:
        return config[key] or dict()
    else:
        return dict()
