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
        """..."""
        pass

    @abstractmethod
    def undeploy_model(self):
        """..."""
        pass

    @classmethod
    @abstractmethod
    def get_running_models(self):
        """..."""
        pass


class Deployment(IDeployment, ABC):
    def __init__(
        self,
        model: str,
        version: str,
        stage: StageType,
    ):
        """[summary]

        Args:
            model (str): [description]
            version (str): [description]
            stage (StageType): [description]
        """
        os.environ['BENTOML_DO_NOT_TRACK'] = 'True'
        self.logger = self.init_logger()
        self.logger.info(f'Initializing {type(self).__name__}: {model}:{version}')
        self.model = model
        self.version = version
        self.stage = stage.value
        for k, v in _get_config('yatai').items():
            os.environ[k] = v

    @abstractmethod
    def deploy_model(self):
        """..."""
        pass

    @abstractmethod
    def undeploy_model(self):
        """..."""
        pass

    @classmethod
    @abstractmethod
    def get_running_models(self):
        """..."""
        pass

    @classmethod
    def init_logger(self) -> logging.Logger:
        """[summary]

        Returns:
            logging.Logger: [description]
        """
        logging.basicConfig(format='[%(asctime)s] %(levelname)s  %(name)s: %(message)s')
        logger = logging.getLogger('coordinator')
        logger.setLevel(logging.DEBUG)
        # ToDo: Attach Handler that raises HTTPExceptions on error or critical?
        return logger

    def get_bentoml_model_by_version(self) -> Tuple[YataiClient, BentoPB]:
        """[summary]

        Raises:
            HTTPException: [description]

        Returns:
            Tuple[YataiClient, BentoPB]: [description]
        """
        yatai_client = get_yatai_client()
        bento_pb = yatai_client.yatai_service.bento_metadata_store.get(self.model, self.version)
        if bento_pb is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'BentoService {self.model}:{self.version} does not exist in BentoML-Repo',
            )
        return yatai_client, bento_pb

    def _is_service_healthy(self, port: int, retries: int) -> bool:
        """[summary]

        Args:
            port (int): [description]
            retries (int): [description]

        Returns:
            bool: [description]
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
        for _ in range(retry):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if s.connect_ex(('localhost', port)) != 0:
                    return False
            time.sleep(1)
        return True
