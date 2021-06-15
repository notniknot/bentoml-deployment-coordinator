import random
import re
import string
from typing import List, Literal

import docker
from bentoml.saved_bundle import safe_retrieve
from bentoml.utils.tempdir import TempDirectory
from docker import DockerClient
from docker.models.containers import Container
from fastapi import HTTPException, status

from app.base_deployment import Deployment
from app.models import Stage, StageType
from app.utils import _distinct


class DockerDeployment(Deployment):
    def __init__(self, model: str, version: str = '', stage: StageType = Stage.NONE):
        """Create instance of docker deployment technique.

        Args:
            model (str): Name of the model.
            version (str, optional): Version of the model. Defaults to ''.
            stage (StageType, optional): New stage of the model. Defaults to Stage.NONE.
        """
        super().__init__(model=model, stage=stage, version=version)
        model_clean = re.sub(r'\W+', '', self.model).lower()
        stage_clean = re.sub(r'\W+', '', self.stage).lower()
        random_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        self.image_name = f'bentoml_{model_clean}_{stage_clean}:{random_string}'  # ToDo: Version???
        self.image_name_general = f'bentoml_{model_clean}_{stage_clean}'
        self.container_name = (
            f'bentoml_{model_clean}_{stage_clean}_{random_string}'  # ToDo: Random String necessary?
        )
        self.container_name_general = f'bentoml_{model_clean}_{stage_clean}'

    def deploy_model(self, port: int, workers: int):
        """Deploy model in docker container.

        Args:
            port (int): Port to forward.
            workers (int): Number of workers to spawn.

        Raises:
            HTTPException: If port is already in use.
        """
        docker_client = docker.from_env()
        stopped_containers = self._stop_model_server(
            docker_client, find_by=['version', 'stage'], remove_container=False
        )
        if self._is_port_in_use(port, 4):
            self.logger.error(f'Port {port} is already in use. Cleaning up...')
            self._start_model_server(docker_client, existing_containers=stopped_containers)
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY, detail=f'Port {port} is already in use.'
            )
        self._start_model_server(docker_client, port=port, workers=workers)
        self._stop_model_server(
            docker_client,
            find_by=['version', 'stage'],
            remove_container=True,
            exclude=self.container_name,
        )
        # ToDo: 'Unrecognized response type; displaying content as text.'
        return 'Deployed model'

    def undeploy_model(self):
        """Undeploy model from docker container.

        Raises:
            HTTPException: If container could not be stopped.
        """
        docker_client = docker.from_env()
        stopped_containers = self._stop_model_server(
            docker_client, find_by=['version'], remove_container=True
        )
        if len(stopped_containers) > 0:
            self.logger.info(f'Undeployed model (docker): {self.model}, {self.version}')
            return 'Successfully undeployed model'
        else:
            self.logger.info(f'Model could not be undeployed: {self.model}, {self.version}')
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f'Model could not be undeployed: {self.model}, {self.version}',
            )

    @classmethod
    def get_running_models(self) -> List[dict]:
        """Get running models in docker containers.

        Returns:
            List[dict]: Information about running models.
        """
        logger = self.init_logger()
        docker_client = docker.from_env()
        containers = docker_client.containers.list(filters={'name': 'bentoml_'})
        containers_fmt = []
        for container in containers:
            labels = ['name', 'version', 'stage', 'port', 'workers']
            if not all(label in labels for label in container.labels):
                continue
            containers_fmt.append({label: container.labels[label] for label in labels})
        logger.info(f'Running model containers: {str(containers_fmt)}')
        return containers_fmt

    def _start_model_server(
        self,
        docker_client: DockerClient,
        existing_containers: list = None,
        port: int = None,
        workers: int = None,
    ):
        """Build and run the docker container.

        Args:
            docker_client (DockerClient): Docker client to use.
            existing_containers (list, optional): List of existing containers that should be restarted. Defaults to None.
            port (int, optional): Port to forward. Defaults to None.
            workers (int, optional): Number of workers to spawn. Defaults to None.

        Raises:
            HTTPException: If docker container could not be built or run.
        """
        if isinstance(existing_containers, list):
            for existing_container in existing_containers:
                existing_container.start()
            for existing_container in existing_containers:
                if self._is_service_healthy(int(existing_container.labels['port']), 7):
                    self.logger.debug(f'Restarted exited container: {existing_container.name}')
                else:
                    self.logger.debug(
                        f'Could not restart exited container: {existing_container.name}'
                    )
            if len(existing_containers) == 0:
                self.logger.debug('No exited containers for restart found.')
            return

        yatai_client, bento_pb = self.get_bentoml_model_by_version()
        with TempDirectory() as temp_dir:
            temp_bundle_path = f'{temp_dir}/{bento_pb.name}'
            bento_service_bundle_path = yatai_client.yatai_service.repo.get(
                bento_pb.name, bento_pb.version
            )
            safe_retrieve(bento_service_bundle_path, temp_bundle_path)
            try:
                docker_client.images.build(path=temp_bundle_path, tag=self.image_name, rm=True)
                self.logger.debug(f'Built image {self.image_name}.')
                docker_client.containers.run(
                    image=self.image_name,
                    name=self.container_name,
                    command=f'--workers={workers}',
                    ports={5000: port},
                    labels={
                        'name': self.model,
                        'version': self.version,
                        'stage': self.stage,
                        'port': str(port),
                        'workers': str(workers),
                    },
                    detach=True,
                )
                self.logger.debug(f'Spinned up container {self.container_name}.')
            except docker.errors.APIError as error:
                self.logger.error(f'Docker server returned an error: {error}')
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f'Docker server returned an error: {error}',
                )
            except docker.errors.BuildError as error:
                self.logger.error(f'Encounter container building issue: {error}')
            except docker.errors.ImageNotFound as error:
                self.logger.error(
                    f'The specified image ({self.image_name}) does not exist: {error}'
                )
            except docker.errors.ContainerError as error:
                self.logger.error(f'The container exited with a non-zero exit code: {error}')

        if not self._is_service_healthy(port, 7):
            self.logger.info('Could not deploy service: ...')
            container = docker_client.containers.get(self.container_name)
            logs = container.logs().decode()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'Could not deploy service.\n{logs}',
            )

    def _stop_model_server(
        self,
        docker_client: DockerClient,
        find_by: List[Literal['version', 'stage']],
        remove_container: bool,
        exclude: str = '',
    ) -> List[Container]:
        """Stop and (if required) remove the docker container.

        Args:
            docker_client (DockerClient): Docker client to use.
            find_by (List[Literal[): Search containers by 'version' and/or 'stage'.
            remove_container (bool): Remove container(s).
            exclude (str, optional): Exclude container by name from search. Defaults to ''.

        Returns:
            List[Container]: List of stopped containers.
        """
        self.logger.debug(
            f'Stopping possible running model server, remove_container={remove_container}.'
        )

        containers = []
        if 'version' in find_by:
            containers += docker_client.containers.list(
                all=True, filters={'label': [f'name={self.model}', f'version={self.version}']}
            )
        if 'stage' in find_by:
            containers += docker_client.containers.list(
                all=True, filters={'label': [f'name={self.model}', f'stage={self.stage}']}
            )

        stopped_containers = []
        for container in _distinct(containers, 'id'):
            if container.name == exclude:
                continue
            if container.status == 'running':
                container.stop(timeout=10)
                stopped_containers.append(container)
                self.logger.debug(f'Stopped container: {container.name}')
            if remove_container:
                self.logger.debug(f'Removing container: {container.name}')
                container.remove()
                self.logger.debug(f'Removing associated image: {container.image.tags[0]}')
                docker_client.images.remove(image=container.attrs['Config']['Image'])
        if len(containers) == 0:
            self.logger.debug(f'No running containers found for {self.model} (searched {find_by})')
        return stopped_containers
