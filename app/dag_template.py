import logging
import os
import re
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict

import requests
import yaml
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException, AirflowNotFoundException
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySizeSensor
from airflow.utils.dates import days_ago

dag_id = Path(__file__).stem


def read_config() -> Dict[str, Any]:
    """
    #### Extract task
    A simple Extract task to get data ready for the rest of the data
    pipeline. In this case, getting data is simulated by reading from a
    hardcoded JSON string.
    """
    dag_file_path = Path(__file__)
    config_file_path = dag_file_path.parent / f'{dag_file_path.stem}.yaml'
    if config_file_path.is_file():
        logging.info(f'Loading config from "{str(config_file_path)}"')
        with config_file_path.open('r') as config_file:
            config = yaml.safe_load(config_file)
        # ? Compare with schema?
        if 'airflow' in config:
            return config['airflow']
        raise AirflowFailException(
            f'Key "airflow" not found in config ({str(config_file_path.name)}).'
        )
    raise AirflowFailException(f'Config "{str(config_file_path.name)}" not found.')


config = read_config()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# ToDo: Add execution timeout
@dag(
    dag_id,
    default_args=default_args,
    catchup=False,
    schedule_interval=config['schedule_interval'],
    # schedule_interval='*/2 * * * *',
    start_date=days_ago(2),
    tags=['bentoml'],
    params=config,
)
def generic_taskflow_dag():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """

    # Step 1
    def check_dependencies():
        def check_fn(data) -> bool:
            context = get_current_context()
            deps = context['params']['s3_dependencies']

            logging.info(f'Found markers: {data}')
            # [{'Key': 'feast_logo.png', 'LastModified': datetime.datetime(2021, 7, 15, 16, 10, 43, 630000, tzinfo=tzlocal()), 'ETag': '"b1d4e05a42da5fd5051330eeb1839997"', 'Size': 3686, 'StorageClass': 'STANDARD', 'Owner': {'DisplayName': 'minio', 'ID': '02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4'}}]
            return any(
                f['Size'] >= int(deps['min_size'])
                and (context['execution_date'] - f['LastModified']).minutes
                < int(deps['max_minutes_ago'])
                for f in data
                if isinstance(f, dict)
            )

        deps = config['s3_dependencies']
        if deps['check_for_marker_key'] is None:
            return DummyOperator(task_id='check_dependencies_dummy')
        else:
            return S3KeySizeSensor(
                task_id='check_dependencies',
                aws_conn_id='minio',
                bucket_name='deployments',
                bucket_key='{{params.s3_dependencies.check_for_marker_key}}',
                check_fn=check_fn,
                soft_fail=True,
                poke_interval=10,
                timeout=deps['poke_timeout'],
            )

    # Step 2
    @task()
    def load_files_from_s3():
        context = get_current_context()
        dag_id = context['dag'].dag_id
        config = context['params']
        s3 = S3Hook(aws_conn_id='minio')
        base_path = Path(os.environ.get('CONTAINERS_PATH', '/opt/airflow/containers'))

        files_to_download = config['files_to_download']
        if files_to_download is None or not isinstance(files_to_download, dict):
            logging.info('No files to download.')
            return

        for source, target in files_to_download.items():
            target = Path(target)
            target_path = base_path / dag_id / target.parent
            target_path.mkdir(parents=True, exist_ok=True)
            logging.info(f'Downloading "{source}" from S3 to "{str(target_path)}".')
            file_path = Path(
                s3.download_file(source, bucket_name='deployments', local_path=str(target_path))
            )
            logging.info(f'Renaming "{file_path.name}" to "{Path(target).name}".')
            file_path.rename(file_path.parent / target.name)

    # Step 3
    @task()
    def call_rest_api():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        config = get_current_context()['params']
        http_config = config['http']
        url = http_config['host'].rstrip('/') + '/' + http_config['endpoint'].lstrip('/')
        logging.info(f'Calling "{url}".')
        response = requests.post(url, data=http_config['data'], timeout=http_config['timeout'])
        if response.status_code == 200:
            logging.info(f'REST Call was successful: {response.text}')
        else:
            raise AirflowException(f'REST Call was not successful: {response.text}')

    # Step 4
    @task()
    def load_files_to_s3():
        context = get_current_context()
        dag_id = context['dag'].dag_id
        config = context['params']
        s3 = S3Hook(aws_conn_id='minio')
        base_path = Path(os.environ.get('CONTAINERS_PATH', '/opt/airflow/containers'))
        s3_prefix = Path('{model}/{stage}'.format(model=config['model'], stage=config['stage']))

        files_to_upload = config['files_to_upload']
        if files_to_upload is None or not isinstance(files_to_upload, dict):
            logging.info('No files to upload.')
            return

        local_target_path = base_path / dag_id
        if not local_target_path.is_dir():
            raise AirflowNotFoundException(f'Path "{str(local_target_path)}" not found.')

        for source, target in files_to_upload.items():
            source = re.sub(r'^/?data/', '', source)
            local_file_to_upload = local_target_path / source
            if not local_file_to_upload.is_file():
                raise AirflowNotFoundException(
                    f'File {str(local_file_to_upload)} not found for uploading!'
                )
            logging.info(f'Uploading "{str(local_file_to_upload)}" to S3.')
            s3.load_file(
                filename=str(local_file_to_upload),
                key=str(s3_prefix / target),
                bucket_name='deployments',
                replace=True,
            )
            # ? Delete files after upload?

        if config['set_marker_after_upload'] is not None:
            s3.load_string(
                string_data='Finished uploading.',
                key=str(s3_prefix / config['set_marker_after_upload']),
                bucket_name='deployments',
                replace=True,
            )
            logging.info(f'Set marker {str(s3_prefix / config["set_marker_after_upload"])}.')

    # Step 5
    def remove_marker_handler():
        @task()
        def remove_marker():
            config = get_current_context()['params']
            s3 = S3Hook(aws_conn_id='minio')

            marker_key = config['s3_dependencies']['check_for_marker_key']
            key = s3.get_wildcard_key(
                wildcard_key=marker_key,
                bucket_name='deployments',
            )
            if key is None:
                raise AirflowNotFoundException(f'Marker {marker_key} not found.')
            else:
                key.delete()
                logging.info(f'Deleted marker {marker_key}.')

        if config['s3_dependencies']['check_for_marker_key'] is None:
            return DummyOperator(task_id='remove_marker_dummy')
        else:
            return remove_marker()

    # Step 1
    check_dependencies_output = check_dependencies()
    # Step 2
    load_files_from_s3_ouput = load_files_from_s3()
    # Step 3
    rest_call_ouput = call_rest_api()
    # Step 4
    load_files_to_s3_output = load_files_to_s3()
    # Step 5
    remove_marker_output = remove_marker_handler()

    # Execution Order
    (
        check_dependencies_output
        >> load_files_from_s3_ouput
        >> rest_call_ouput
        >> load_files_to_s3_output
        >> remove_marker_output
    )


dag_object = generic_taskflow_dag()
