import os
from typing import Any, List, Union

import yaml


def _get_config(key_or_path: Union[str, tuple]) -> dict:
    """Read config file.

    Args:
        key (str or tuple): Key/Path to look for in config file.

    Returns:
        dict: Config by key/path.
    """
    current_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(current_dir, 'config.yaml')
    if not os.path.exists(config_path):
        return dict()
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    if isinstance(key_or_path, str) and key_or_path in config:
        return config[key_or_path] or dict()
    elif isinstance(key_or_path, tuple):
        for path_ele in key_or_path:
            if config is None:
                break
            if path_ele in config:
                config = config[path_ele]
        else:
            return config
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
