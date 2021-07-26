import logging
import re
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Tuple

import yaml
from airflow.decorators import dag
from airflow.exceptions import AirflowFailException
from airflow.utils.dates import days_ago

dag_id = Path(__file__).stem


def read_config() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    #### Extract task
    A simple Extract task to get data ready for the rest of the data
    pipeline. In this case, getting data is simulated by reading from a
    hardcoded JSON string.
    """
    dag_file_path = Path(__file__)
    config_file_path = dag_file_path.parent / f'{dag_file_path.stem}.yaml'
    if not config_file_path.is_file():
        raise AirflowFailException(f'Config "{str(config_file_path.name)}" not found.')

    logging.info(f'Loading config from "{str(config_file_path)}"')
    with config_file_path.open('r') as config_file:
        config = yaml.safe_load(config_file)
    if 'airflow' not in config:
        raise AirflowFailException(
            f'Key "airflow" not found in config ({str(config_file_path.name)}).'
        )
    config = config['airflow']
    task_re = re.compile(r'^task_\d+$')

    def is_task(k, v):
        return isinstance(v, dict) and task_re.match(k) and 'type' in v

    tasks = {k: config[k] for k in sorted({k: v for k, v in config.items() if is_task(k, v)})}
    general = {k: v for k, v in config.items() if not task_re.match(k)}
    return tasks, general


tasks, general = read_config()

default_args = {
    'owner': general['owner'],
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(seconds=general['execution_timeout']),
}


@dag(
    dag_id,
    default_args=default_args,
    catchup=False,
    schedule_interval=general['schedule_interval'],
    start_date=days_ago(2),
    tags=['bentoml'],
)
def generic_taskflow_dag():
    def get_func(type: str):
        # fmt: off
        if type == 's3_dependency':
            from custom_operators.CustomS3FileSensor import CustomS3FileSensor
            return CustomS3FileSensor
        elif type == 's3_download':
            from custom_operators.CustomS3FileDownloadOperator import \
                CustomS3FileDownloadOperator
            return CustomS3FileDownloadOperator
        elif type == 's3_upload':
            from custom_operators.CustomS3FileUploadOperator import \
                CustomS3FileUploadOperator
            return CustomS3FileUploadOperator
        elif type == 's3_remove':
            from custom_operators.CustomS3FileRemovalOperator import \
                CustomS3FileRemovalOperator
            return CustomS3FileRemovalOperator
        elif type == 'http':
            from custom_operators.CustomHTTPOperator import CustomHTTPOperator
            return CustomHTTPOperator
        elif type == 'file_move':
            from custom_operators.CustomFileMovementOperator import \
                CustomFileMovementOperator
            return CustomFileMovementOperator
        # fmt: on

    operators = []
    for task_no, kwargs in tasks.items():
        type = kwargs['type']
        cls_obj = get_func(type)
        kwargs['task_id'] = f'{task_no}-{type}'
        kwargs['general_args'] = general
        del kwargs['type']
        operators.append(cls_obj(**kwargs))

    if len(operators) == 0:
        raise Exception()
    elif len(operators) == 1:
        pass  # ToDo
    else:
        for i in range(1, len(operators)):
            operators[i - 1] >> operators[i]


dag_object = generic_taskflow_dag()
