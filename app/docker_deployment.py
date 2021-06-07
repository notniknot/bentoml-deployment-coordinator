import random
import re
import string

import docker
from bentoml.exceptions import YataiRepositoryException
from bentoml.saved_bundle import safe_retrieve
from bentoml.utils.tempdir import TempDirectory
from bentoml.yatai.client import get_yatai_client
from docker import DockerClient
from fastapi import HTTPException, status

from app.base_deployment import Deployment
from app.models import Stage, StageType


class DockerDeployment(Deployment):
    def __init__(self, model: str, version: str = '', stage: StageType = Stage.NONE):
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
        docker_client = docker.from_env()
        # ToDo: Return List with stopped containers
        self._stop_model_server(docker_client, remove_container=False)
        if self._is_port_in_use(port, 4):
            self.logger.error(f'Port {port} is already in use. Cleaning up...')
            # ToDo: Give list
            self._start_model_server(docker_client, None, None, existing_container=True)
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY, detail=f'Port {port} is already in use.'
            )
        self._start_model_server(docker_client, port, workers)
        # ToDo: Give list ???
        self._stop_model_server(
            docker_client,
            remove_container=True,
            exclude=self.container_name,
        )
        return super().deploy_model()

    def undeploy_model(self):
        docker_client = docker.from_env()
        self._stop_model_server(docker_client, remove_container=True)
        return super().undeploy_model()

    def _start_model_server(
        self, docker_client: DockerClient, port: int, workers: int, existing_container: bool = False
    ):
        if existing_container:
            containers = docker_client.containers.list(
                all=True, filters={'name': self.container_name_general}
            )
            if len(containers) > 0 and containers[0].status == 'exited':
                containers[0].start()
                self.logger.debug(f'Restarted exited container: {containers[0].name}')
                return True
            else:
                self.logger.debug('Old exited containers could not be found.')
                return False

        # ToDo Call
        yatai_client = get_yatai_client()
        bento_pb = yatai_client.yatai_service.bento_metadata_store.get(self.model, self.version)
        if not bento_pb:
            raise YataiRepositoryException(
                f'BentoService {self.model}:{self.version} ' f'does not exist'
            )
        with TempDirectory() as temp_dir:
            temp_bundle_path = f'{temp_dir}/{bento_pb.name}'
            # bento_service_bundle_path = bento_pb.uri.uri
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
                raise YataiRepositoryException(error)
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
        self, docker_client: DockerClient, remove_container: bool, exclude: str = ''
    ):
        self.logger.debug(
            f'Stopping possible running model server, remove_container={remove_container}.'
        )
        containers = docker_client.containers.list(
            all=True, filters={'name': self.image_name_general}
        )
        # docker_client.containers.list(
        #     all=True, filters={'label': ['name=IrisClassifier', 'version=20210603092404_36AE78']}
        # )
        # docker_client.containers.list(
        #     all=True, filters={'label': ['name=IrisClassifier', 'stage=staging']}
        # )
        for container in containers:
            if container.name == exclude:
                continue
            if container.status == 'running':
                container.stop(timeout=10)
                self.logger.debug(f'Stopped container: {container.name}')
            if remove_container:
                self.logger.debug(f'Removing container: {container.name}')
                container.remove()
                self.logger.debug('Removing associated image.')
                docker_client.images.remove(image=container.attrs['Config']['Image'])
        if len(containers) == 0:
            self.logger.debug(f'No running containers found: {self.container_name_general}')
            return False
        else:
            return True

    @classmethod
    def get_running_models(self):
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
