import os
from typing import Any, List

import yaml


def _get_config(key: str) -> dict:
    """Read config file.

    Args:
        key (str): Key to look for in config file.

    Returns:
        dict: Config by key.
    """
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


def _distinct(obj_list: List[Any], attr: str) -> List[Any]:
    """Reduce list of unhashable objects to distinct occurences.

    Args:
        obj_list (List[Any]): List of unhashable objects.
        attr (str): Attribute for comparison.

    Returns:
        List[Any]: Reduced list of distinct objects.
    """
    distinct_obj_list = []
    for obj in obj_list:
        for distinct_obj in distinct_obj_list:
            if getattr(obj, attr) == getattr(distinct_obj, attr):
                break
        else:
            distinct_obj_list.append(obj)
    return distinct_obj_list
