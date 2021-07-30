import logging
import os
import re
import shutil
from pathlib import Path
from typing import List

import docker
import yaml
from fastapi import HTTPException, status

from app.base_deployment import Deployment
from app.models import Stage
from app.utils import _get_config

logger = logging.getLogger(f'coordinator.{__name__}')


class AirflowDeployment(Deployment):
    def __init__(
        self,
        airflow: dict,
        name: str,
        version: str = None,
        suffix: str = None,
        stage: Stage = Stage.NONE,
    ):
        """Create instance of base deployment technique.

        Args:
            model (str): Name of the model.
            version (str): Version of the model.
            stage (Stage): New stage of the model.
        """
        super().__init__(name=name, stage=stage, version=version, suffix=suffix)
        self.airflow = {'airflow': airflow}
        self.dag_location = Path(_get_config(('airflow', 'dag_location')))

    def deploy_model(self):
        """Abstract method to deploy model."""
        logger.info(f'Deploying {self.deployment_name} to Airflow.')
        self.dag_template = Path(_get_config(('airflow', 'dag_template')))
        if not self.dag_template.is_file():
            raise HTTPException(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'Dag template ({str(self.dag_template)}) not found.',
            )
        self.remove_dag(by=['suffix', 'stage'])
        shutil.copy(str(self.dag_template), str(self.dag_location / f'{self.deployment_name}.py'))
        with open(str(self.dag_location / f'{self.deployment_name}.yaml'), 'w') as file:
            yaml.safe_dump(self.airflow, file)

    def undeploy_model(self, removed_containers: list):
        """Abstract method to undeploy model."""
        for removed_container in removed_containers:
            if removed_container.labels.get('batch_prediction', False) in [True, 'True']:
                self.remove_dag(by=['name'], name=removed_container.name)

    def remove_dag(self, by: List[str], name: str = None):
        def remove_by(by_regex: re.Pattern = None, by_name: str = None):
            dags = set()
            extension = re.compile(r'(.yaml|.py)$')
            if by_regex is not None:
                for entry in os.scandir(self.dag_location):
                    if entry.is_file() and by_regex.match(entry.name):
                        os.remove(entry.path)
                        logger.info(f'Deleted file: {entry.name}')
                        dags.add(extension.sub('', entry.name))
            if by_name is not None:
                dags.add(by_name)
                dag_path = self.dag_location / f'{by_name}.py'
                if dag_path.is_file():
                    os.remove(str(dag_path))
                    logger.info(f'Deleted DAG file: {str(dag_path)}')
                else:
                    logger.warning(f'DAG file could not be found: {str(dag_path)}')
                yaml_path = self.dag_location / f'{by_name}.yaml'
                if yaml_path.is_file():
                    os.remove(str(yaml_path))
                    logger.info(f'Deleted YAML file: {str(yaml_path)}')
                else:
                    logger.warning(f'YAML file could not be found: {str(yaml_path)}')
            return dags

        dags = set()
        if 'suffix' in by:
            regex_by_suffix = re.compile(
                r'^{}_{}_\w+_{}.(?:py|yaml)$'.format(
                    AirflowDeployment.prefix, self.name_clean, self.suffix
                )
            )
            tmp_dags = remove_by(by_regex=regex_by_suffix)
            dags.update(tmp_dags)
        if 'stage' in by:
            regex_by_stage = re.compile(
                r'^{}_{}_{}_\w+.(?:py|yaml)$'.format(
                    AirflowDeployment.prefix, self.name_clean, self.stage_clean
                )
            )
            tmp_dags = remove_by(by_regex=regex_by_stage)
            dags.update(tmp_dags)
        if 'name' in by:
            if name is None:
                raise ValueError('Parameter "name" cannot be None')
            tmp_dags = remove_by(by_name=name)
            dags.update(tmp_dags)

        docker_client = docker.from_env()
        airflow_container_id = _get_config(('airflow', 'container_id'))
        airflow_container = docker_client.containers.get(airflow_container_id)

        for dag in dags:
            output = airflow_container.exec_run(
                f'airflow dags delete -y {dag}', stderr=True, stdout=True
            )
            if output.exit_code != 0:
                logger.warning(f'Airflow could not delete DAG for {dag}: {output.output.decode()}')
            else:
                logger.info(f'Deleted Airflow DAG: {dag}')

    @classmethod
    def get_running_models(cls):
        """Abstract method to get running models."""
        dags = []
        dag_location = Path(_get_config(('airflow', 'dag_location')))
        regex_all = re.compile(r'^{}_\w+_\w+_\w+.yaml$'.format(cls.prefix))
        for entry in os.scandir(dag_location):
            if entry.is_file() and regex_all.match(entry.name):
                with open(entry.path, 'r') as file:
                    config = yaml.safe_load(file)['airflow']
                attrs = ['model', 'stage', 'version']
                dags.append({k: v for k, v in config.items() if k in attrs})
        return dags
