import logging
import os
import socket
import time
from abc import ABC, abstractmethod
from typing import Tuple

import requests
from bentoml.yatai.client import YataiClient, get_yatai_client
from bentoml.yatai.proto.repository_pb2 import Bento as BentoPB
from fastapi import HTTPException, status
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.models import StageType
from app.utils import _get_config


class IDeployment(ABC):
    @abstractmethod
    def deploy_model(self):
        """Abstract method to deploy model."""
        pass

    @abstractmethod
    def undeploy_model(self):
        """Abstract method to undeploy model."""
        pass

    @classmethod
    @abstractmethod
    def get_running_models(self):
        """Abstract method to get running models."""
        pass


class Deployment(IDeployment, ABC):
    def __init__(
        self,
        name: str,
        version: str,
        stage: StageType,
    ):
        """Create instance of base deployment technique.

        Args:
            model (str): Name of the model.
            version (str): Version of the model.
            stage (StageType): New stage of the model.
        """
        os.environ['BENTOML_DO_NOT_TRACK'] = 'True'
        self.logger = self.init_logger()
        self.logger.info(f'Initializing {type(self).__name__}: {name}:{version}')
        self.name = name
        self.version = version
        self.stage = stage.value
        for k, v in _get_config('yatai').items():
            os.environ[k] = v

    @abstractmethod
    def deploy_model(self):
        """Abstract method to deploy model."""
        pass

    @abstractmethod
    def undeploy_model(self):
        """Abstract method to undeploy model."""
        pass

    @classmethod
    @abstractmethod
    def get_running_models(self):
        """Abstract method to get running models."""
        pass

    @classmethod
    def init_logger(self) -> logging.Logger:
        """Configure root logger and set log level for coordinator logger.

        Returns:
            logging.Logger: Logging instance of 'coordinator'.
        """
        logging.basicConfig(format='[%(asctime)s] %(levelname)s  %(name)s: %(message)s')
        logger = logging.getLogger('coordinator')
        logger.setLevel(logging.DEBUG)
        # ToDo: Attach Handler that raises HTTPExceptions on error or critical?
        return logger

    def get_bentoml_model_by_version(self) -> Tuple[YataiClient, BentoPB]:
        """Retrieve model information from BentoML-Repository.

        Raises:
            HTTPException: If model could not be retrieved.

        Returns:
            Tuple[YataiClient, BentoPB]: YataiClient for further operations, BentoProtoBuffer.
        """
        yatai_client = get_yatai_client()
        bento_pb = yatai_client.yatai_service.bento_metadata_store.get(self.name, self.version)
        if bento_pb is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'BentoService {self.name}:{self.version} does not exist in BentoML-Repo',
            )
        return yatai_client, bento_pb

    def _is_service_healthy(self, port: int, retries: int) -> bool:
        """Checks healthz endpoint of BentoML model for life.

        Args:
            port (int): Port of the deployed model.
            retries (int): Number of retries before giving up.

        Returns:
            bool: Whether service is reachable or not.
        """
        self.logger.debug('Checking for service health.')
        logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
        session = requests.Session()
        retries = Retry(total=retries, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        try:
            response = session.get(f'http://localhost:{port}/healthz', timeout=1)
            if response.status_code == 200:
                self.logger.debug('Service up and running.')
                return True
        except Exception:
            self.logger.debug('Health check unsuccessful.')
            return False

    def _is_port_in_use(self, port: int, retry: int = 3) -> bool:
        """Checks if a given port is already in use.

        Args:
            port (int): Given port to check.
            retry (int, optional): Number of retries to check. Defaults to 3.

        Returns:
            bool: Whether port is in use or not.
        """
        for _ in range(retry):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if s.connect_ex(('localhost', port)) != 0:
                    return False
            time.sleep(1)
        return True
