import json
import random
import re
import string
from typing import List, Literal

import docker
from bentoml.saved_bundle import safe_retrieve
from bentoml.utils.tempdir import TempDirectory
from bentoml.yatai.deployment.docker_utils import ensure_docker_available_or_raise
from docker import DockerClient
from docker.models.containers import Container
from docker.types import Ulimit
from fastapi import HTTPException, status

from app.base_deployment import Deployment
from app.models import Stage
from app.utils import _distinct, _get_config

DOCKER_TIMEOUT = 120
STANDARD_PORT = 5000


class DockerDeployment(Deployment):
    def __init__(self, name: str, version: str = '', stage: Stage = Stage.NONE):
        """Create instance of docker deployment technique.

        Args:
            model (str): Name of the model.
            version (str, optional): Version of the model. Defaults to ''.
            stage (Stage, optional): New stage of the model. Defaults to Stage.NONE.
        """
        super().__init__(name=name, stage=stage, version=version)
        name_clean = re.sub(r'\W+', '', self.name).lower()
        stage_clean = re.sub(r'\W+', '', self.stage).lower()
        random_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        self.image_name = f'bentoml_{name_clean}_{stage_clean}:{random_string}'
        self.image_name_general = f'bentoml_{name_clean}_{stage_clean}'
        self.container_name = f'bentoml_{name_clean}_{stage_clean}_{random_string}'
        self.container_name_general = f'bentoml_{name_clean}_{stage_clean}'
        ensure_docker_available_or_raise()

    def deploy_model(self, args: dict):
        """Deploy model in docker container.

        Args:
            args (dict): Dictionary containing all the  arguments for the bentoml call.

        Raises:
            HTTPException: If port is already in use.
        """
        docker_client = docker.from_env()
        stopped_containers = self._stop_model_server(
            docker_client, find_by=['version', 'stage'], remove_container=False
        )
        port = args['port']
        if self._is_port_in_use(port, 4):
            self.logger.error(f'Port {port} is already in use. Cleaning up...')
            self._start_model_server(docker_client, args, existing_containers=stopped_containers)
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY, detail=f'Port {port} is already in use.'
            )
        self._start_model_server(docker_client, args)
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
            self.logger.info(f'Undeployed model (docker): {self.name}, {self.version}')
            return 'Successfully undeployed model'
        else:
            self.logger.info(f'Model could not be undeployed: {self.name}, {self.version}')
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f'Model could not be undeployed: {self.name}, {self.version}',
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
            labels = ['name', 'version', 'stage']
            if not all(label in container.labels for label in labels):
                continue
            container_labels = {label: container.labels[label] for label in labels}
            container_labels['args'] = container.labels.get('args', dict())
            containers_fmt.append(container_labels)
        logger.info(f'Running model containers: {str(containers_fmt)}')
        return containers_fmt

    def _start_model_server(
        self, docker_client: DockerClient, args: dict, existing_containers: list = None
    ):
        """Build and run the docker container.

        Args:
            docker_client (DockerClient): Docker client to use.
            args (dict): Dictionary containing all the  arguments for the bentoml call.
            existing_containers (list, optional): List of existing containers that should be restarted. Defaults to None.

        Raises:
            HTTPException: If docker container could not be built or run.
        """
        if isinstance(existing_containers, list):
            for existing_container in existing_containers:
                existing_container.start()
            for existing_container in existing_containers:
                port = int(json.loads(existing_container.labels['args'])['port'])
                if self._is_service_healthy(port, 20):
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
                self.logger.debug(f'Building image for {self.name}:{self.version}.')
                build_args = _get_config(('docker', 'build_args'))
                docker_client.images.build(
                    path=temp_bundle_path,
                    tag=self.image_name,
                    buildargs=build_args,
                    rm=True,
                    forcerm=True,
                    timeout=DOCKER_TIMEOUT,
                )
                self.logger.debug(f'Built image {self.image_name}.')
            except (docker.errors.BuildError, docker.errors.APIError, TypeError) as error:
                error_msg = f'Docker returned an error when building the image ({type(error).__name__}): {error}'
                self.logger.error(error_msg)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=error_msg
                )

            try:
                docker_client.containers.run(
                    image=self.image_name,
                    name=self.container_name,
                    command=self.get_bentoml_args(args),
                    ports={args['port']: args['port']},
                    labels={
                        'name': self.name,
                        'version': self.version,
                        'stage': self.stage,
                        'args': json.dumps(args),
                    },
                    ulimits=[Ulimit(name='core', soft=0, hard=0)],
                    detach=True,
                )
                self.logger.debug(f'Spinned up container {self.container_name}.')
            except (
                docker.errors.ContainerError,
                docker.errors.ImageNotFound,
                docker.errors.APIError,
            ) as error:
                error_msg = f'Docker returned an error when running the container ({type(error).__name__}): {error}'
                self.logger.error(error_msg)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=error_msg
                )

        if not self._is_service_healthy(args['port'], 20):
            self.logger.info(f'Could not deploy service for {self.container_name}')
            container = docker_client.containers.get(self.container_name)
            logs = container.logs().decode()
            self.logger.info('Removing container.')
            container.stop(timeout=10)
            container.remove()
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
                all=True, filters={'label': [f'name={self.name}', f'version={self.version}']}
            )
        if 'stage' in find_by:
            containers += docker_client.containers.list(
                all=True, filters={'label': [f'name={self.name}', f'stage={self.stage}']}
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
            self.logger.debug(f'No running containers found for {self.name} (searched {find_by})')
        return stopped_containers
