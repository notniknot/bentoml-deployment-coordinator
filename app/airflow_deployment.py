import logging
import os
import re
import shutil
from pathlib import Path

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
        self.dag_template = Path(_get_config(('airflow', 'dag_template')))
        if not self.dag_template.is_file():
            raise HTTPException(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'Dag template ({str(self.dag_template)}) not found.',
            )

        regex_by_suffix = re.compile(
            r'^{}_{}_\w+_{}.(?:py|yaml)$'.format(self.prefix, self.name_clean, self.suffix)
        )
        regex_by_stage = re.compile(
            r'^{}_{}_{}_\w+.(?:py|yaml)$'.format(self.prefix, self.name_clean, self.stage_clean)
        )
        for entry in os.scandir(self.dag_location):
            if entry.is_file() and (
                regex_by_suffix.match(entry.name) or regex_by_stage.match(entry.name)
            ):
                os.remove(entry.path)

        shutil.copy(str(self.dag_template), str(self.dag_location / f'{self.deployment_name}.py'))
        with open(str(self.dag_location / f'{self.deployment_name}.yaml'), 'w') as file:
            yaml.safe_dump(self.airflow, file)

    def undeploy_model(self, removed_containers: list):
        """Abstract method to undeploy model."""
        docker_client = docker.from_env()
        airflow_container_id = _get_config(('airflow', 'container_id'))
        airflow_container = docker_client.containers.get(airflow_container_id)
        for removed_container in removed_containers:
            dag_path = self.dag_location / f'{removed_container.name}.py'
            if dag_path.is_file():
                os.remove(str(dag_path))
                logger.info(f'Deleted DAG file: {str(dag_path)}')
            else:
                logger.warning(f'DAG file could not be found: {str(dag_path)}')

            yaml_path = self.dag_location / f'{removed_container.name}.yaml'
            if yaml_path.is_file():
                os.remove(str(yaml_path))
                logger.info(f'Deleted YAML file: {str(yaml_path)}')
            else:
                logger.warning(f'YAML file could not be found: {str(yaml_path)}')

            output = airflow_container.exec_run(
                f'airflow dags delete -y {removed_container.name}', stderr=True, stdout=True
            )
            if output.exit_code != 0:
                logger.warning(
                    f'Airflow could not delete DAG for {removed_container.name}: {output.output.decode()}'
                )
            else:
                logger.info(f'Deleted Airflow DAG: {removed_container.name}')

    @classmethod
    def get_running_models(self):
        """Abstract method to get running models."""
        dags = []
        regex_all = re.compile(r'^{}_\w+_\w+_\w+.yaml$'.format(self.prefix))
        for entry in os.scandir(self.dag_location):
            if entry.is_file() and regex_all.match(entry.name):
                with open('entry.path', 'r') as file:
                    config = yaml.safe_load(file)['airflow']
                attrs = ['model', 'stage', 'version']
                dags.append({k: v for k, v in config.items() if k in attrs})
        return dags
