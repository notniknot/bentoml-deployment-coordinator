import random
import re
import string

import docker
from bentoml.exceptions import YataiRepositoryException
from bentoml.saved_bundle import safe_retrieve
from bentoml.utils.tempdir import TempDirectory
from bentoml.yatai.client import get_yatai_client
from fastapi import HTTPException, status

from app.base_deployment import Deployment
from app.models import StageType


# https://docker-py.readthedocs.io/en/stable/
class DockerDeployment(Deployment):
    def __init__(self, model: str, stage: StageType, version: str = ''):
        super().__init__(model=model, stage=stage, version=version)
        model_clean = re.sub(r'\W+', '', self.model).lower()
        stage_clean = re.sub(r'\W+', '', self.stage).lower()
        random_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        self.image_name = f'bentoml_{model_clean}_{stage_clean}:{random_string}'
        self.container_name = f'bentoml_{model_clean}_{stage_clean}_{random_string}'

    def deploy_model(self, port: int, workers: int):
        yatai_client = get_yatai_client()
        bento_pb = yatai_client.yatai_service.bento_metadata_store.get(self.model, self.version)
        if not bento_pb:
            raise YataiRepositoryException(
                f'BentoService {self.model}:{self.version} ' f'does not exist'
            )

        docker_client = docker.from_env()

        with TempDirectory() as temp_dir:
            temp_bundle_path = f'{temp_dir}/{bento_pb.name}'
            # bento_service_bundle_path = bento_pb.uri.uri
            bento_service_bundle_path = yatai_client.yatai_service.repo.get(
                bento_pb.name, bento_pb.version
            )
            safe_retrieve(bento_service_bundle_path, temp_bundle_path)
            try:
                docker_client.images.build(path=temp_bundle_path, tag=self.image_name, rm=True)
                docker_client.containers.run(
                    image=self.image_name,
                    name=self.container_name,
                    command=f'--workers={workers}',
                    ports={5000: port},
                    detach=True,
                )
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
            self.logger.info(f'Could not deploy service: ...')
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'Could not deploy service.\n...',
            )

        return super().deploy_model()

    def undeploy_model(self):
        return super().undeploy_model()

    @classmethod
    def get_running_models(self):
        return super().get_running_models()
