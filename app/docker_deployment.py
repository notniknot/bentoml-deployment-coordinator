import glob
import json
import logging
import os
import shutil
from pathlib import Path
from typing import List, Literal, Optional, Tuple

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

logger = logging.getLogger(f'coordinator.{__name__}')


class DockerDeployment(Deployment):
    def __init__(self, name: str, version: str = '', stage: Stage = Stage.NONE):
        """Create instance of docker deployment technique.

        Args:
            model (str): Name of the model.
            version (str, optional): Version of the model. Defaults to ''.
            stage (Stage, optional): New stage of the model. Defaults to Stage.NONE.
        """
        super().__init__(name=name, stage=stage, version=version)
        self.image_name = f'{self.prefix}_{self.name_clean}_{self.stage_clean}:{self.suffix}'
        ensure_docker_available_or_raise()

    def deploy_model(self, args: dict):
        """Deploy model in docker container.

        Args:
            args (dict): Dictionary containing all the  arguments for the bentoml call.

        Raises:
            HTTPException: If port is already in use.
        """
        docker_client = docker.from_env()
        stopped_containers, _ = self._stop_model_server(
            docker_client, find_by=['version', 'stage'], remove_container=False
        )
        port = args['port']
        if self._is_port_in_use(port, 4):
            logger.error(f'Port {port} is already in use. Cleaning up...')
            self._start_model_server(docker_client, args, existing_containers=stopped_containers)
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY, detail=f'Port {port} is already in use.'
            )
        self._start_model_server(docker_client, args)
        _, removed_containers = self._stop_model_server(
            docker_client,
            find_by=['version', 'stage'],
            remove_container=True,
            exclude=self.deployment_name,
        )
        return {
            'deployment_name': self.deployment_name,
            'suffix': self.suffix,
            'removed_containers': removed_containers,
        }

    def undeploy_model(self):
        """Undeploy model from docker container.

        Raises:
            HTTPException: If container could not be stopped.
        """
        docker_client = docker.from_env()
        stopped_containers, _ = self._stop_model_server(
            docker_client, find_by=['version'], remove_container=True
        )
        if len(stopped_containers) > 0:
            logger.info(f'Undeployed model (docker): {self.name}, {self.version}')
            return stopped_containers
        else:
            logger.info(f'Model could not be undeployed: {self.name}, {self.version}')
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

    def _handle_shared_volumes(self, action: str, deployment_name: str) -> Optional[str]:
        container_location = Path(_get_config(('docker', 'container_location')))
        current_container_location = container_location / deployment_name
        if action == 'create':
            if current_container_location.exists():
                files = glob.glob(f'{str(current_container_location)}/.*')
                for f in files:
                    try:
                        os.remove(f)
                    except Exception:
                        logger.warning(f'Could not delete {f} from {deployment_name}.')
            else:
                try:
                    logger.info(f'Creating shared volume: {str(current_container_location)}')
                    current_container_location.mkdir(parents=True, exist_ok=True)
                except Exception as error:
                    error_msg = f'Could not create shared volume ({type(error).__name__}): {error}'
                    logger.error(error_msg)
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=error_msg
                    )
            return str(current_container_location)
        elif action == 'remove':
            try:
                logger.info(f'Deleting shared volume: {str(current_container_location)}')
                shutil.rmtree(str(current_container_location))
            except Exception as error:
                error_msg = f'Could not delete shared volume ({type(error).__name__}): {error}'
                logger.error(error_msg)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=error_msg
                )
        return None

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
                    logger.debug(f'Restarted exited container: {existing_container.name}')
                else:
                    logger.debug(f'Could not restart exited container: {existing_container.name}')
            if len(existing_containers) == 0:
                logger.debug('No exited containers for restart found.')
            return

        yatai_client, bento_pb = self.get_bentoml_model_by_version()
        with TempDirectory() as temp_dir:
            temp_bundle_path = f'{temp_dir}/{bento_pb.name}'
            bento_service_bundle_path = yatai_client.yatai_service.repo.get(
                bento_pb.name, bento_pb.version
            )
            safe_retrieve(bento_service_bundle_path, temp_bundle_path)

            try:
                logger.debug(f'Building image for {self.name}:{self.version}.')
                build_args = _get_config(('docker', 'build_args'))
                docker_client.images.build(
                    path=temp_bundle_path,
                    tag=f'{self.image_name}-tmp',
                    buildargs=build_args,
                    rm=True,
                    forcerm=True,
                    timeout=DOCKER_TIMEOUT,
                )
                logger.debug(f'Built image {self.image_name}-tmp.')
                # Fix bentoml timeout error
                logger.debug(f'Building fixed image for {self.name}:{self.version}.')
                docker_client.images.build(
                    path=_get_config(('docker', 'bentoml_fix')),
                    tag=self.image_name,
                    buildargs={'BASE_IMAGE': f'{self.image_name}-tmp'},
                    rm=True,
                    forcerm=True,
                    timeout=DOCKER_TIMEOUT,
                )
                logger.debug(f'Built image {self.image_name}.')
            except (docker.errors.BuildError, docker.errors.APIError, TypeError) as error:
                error_msg = f'Docker returned an error when building the image ({type(error).__name__}): {error}'
                logger.error(error_msg)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=error_msg
                )

        container_volume = self._handle_shared_volumes(
            action='create', deployment_name=self.deployment_name
        )
        try:
            # Group set to 1016
            # > id
            # 1016(developer)
            docker_client.containers.run(
                image=self.image_name,
                name=self.deployment_name,
                command=self.get_bentoml_args(args),
                ports={args['port']: args['port']},
                volumes={container_volume: {'bind': '/data', 'mode': 'z'}},
                user='bentoml:1016',
                labels={
                    'name': self.name,
                    'version': self.version,
                    'stage': self.stage,
                    'args': json.dumps(args),
                },
                ulimits=[Ulimit(name='core', soft=0, hard=0)],
                detach=True,
            )
            logger.debug(f'Spinned up container {self.deployment_name}.')
        except (
            docker.errors.ContainerError,
            docker.errors.ImageNotFound,
            docker.errors.APIError,
        ) as error:
            error_msg = f'Docker returned an error when running the container ({type(error).__name__}): {error}'
            logger.error(error_msg)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=error_msg)

        if not self._is_service_healthy(args['port'], 20):
            logger.info(f'Could not deploy service for {self.deployment_name}')
            container = docker_client.containers.get(self.deployment_name)
            logs = container.logs().decode()
            logger.info('Removing container.')
            container.stop(timeout=10)
            container.remove()
            self._handle_shared_volumes(action='remove', deployment_name=self.deployment_name)
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
    ) -> Tuple[List[Container], List[Container]]:
        """Stop and (if required) remove the docker container.

        Args:
            docker_client (DockerClient): Docker client to use.
            find_by (List[Literal[): Search containers by 'version' and/or 'stage'.
            remove_container (bool): Remove container(s).
            exclude (str, optional): Exclude container by name from search. Defaults to ''.

        Returns:
            List[Container]: List of stopped containers.
        """
        logger.debug(
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
        removed_containers = []
        for container in _distinct(containers, 'id'):
            deployment_name = container.name
            if deployment_name == exclude:
                continue
            if container.status == 'running':
                container.stop(timeout=10)
                stopped_containers.append(container)
                logger.debug(f'Stopped container: {deployment_name}')
            if remove_container:
                logger.debug(f'Removing container: {deployment_name}')
                container.remove()
                logger.debug(f'Removing associated image: {container.attrs["Config"]["Image"]}')
                docker_client.images.remove(image=container.attrs['Config']['Image'])
                logger.debug(
                    f'Removing associated tmp-image: {container.attrs["Config"]["Image"]}-tmp'
                )
                try:
                    docker_client.images.remove(image=f'{container.attrs["Config"]["Image"]}-tmp')
                except docker.errors.ImageNotFound:
                    pass
                self._handle_shared_volumes(action='remove', deployment_name=deployment_name)
                removed_containers.append(container)
        if len(containers) == 0:
            logger.debug(f'No running containers found for {self.name} (searched {find_by})')
        return stopped_containers, removed_containers
