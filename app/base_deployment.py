import logging
import os
import random
import re
import socket
import string
import time
from abc import ABC, abstractmethod
from typing import Tuple

import requests
from bentoml.yatai.client import YataiClient, get_yatai_client
from bentoml.yatai.locking.lock import LockType, lock
from bentoml.yatai.proto.repository_pb2 import Bento as BentoPB
from fastapi import HTTPException, status
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.models import Stage
from app.utils import _get_config

logger = logging.getLogger(f'coordinator.{__name__}')


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
    def __init__(self, name: str, version: str, stage: Stage, suffix: str = None):
        """Create instance of base deployment technique.

        Args:
            model (str): Name of the model.
            version (str): Version of the model.
            stage (Stage): New stage of the model.
        """
        os.environ['BENTOML_DO_NOT_TRACK'] = 'True'
        logger.info(f'Initializing {type(self).__name__}: {name}:{version}')

        self.name = name
        self.version = version
        self.stage = stage.value
        self.name_clean = re.sub(r'\W+', '', self.name).lower()
        self.stage_clean = re.sub(r'\W+', '', self.stage).lower()

        if suffix is not None:
            self.suffix = suffix
        else:
            self.suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        self.prefix = 'bentoml'
        self.deployment_name = f'{self.prefix}_{self.name_clean}_{self.stage_clean}_{self.suffix}'

        for k, v in _get_config('env_vars').items():
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

    def get_bentoml_model_by_version(self) -> Tuple[YataiClient, BentoPB]:
        """Retrieve model information from BentoML-Repository.

        Raises:
            HTTPException: If model could not be retrieved.

        Returns:
            Tuple[YataiClient, BentoPB]: YataiClient for further operations, BentoProtoBuffer.
        """
        yatai_client = get_yatai_client()
        # ! For Version 0.12
        # bento_pb = yatai_client.yatai_service.bento_metadata_store.get(self.name, self.version)
        # ! For Version 0.13
        db = yatai_client.yatai_service.db
        bento_id = f"{self.name}_{self.version}"
        with lock(db, [(bento_id, LockType.READ)]) as (sess, _):
            bento_pb = db.metadata_store.get(sess, self.name, self.version)
        if bento_pb is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'BentoService {self.name}:{self.version} does not exist in BentoML-Repo',
            )
        return yatai_client, bento_pb

    def get_bentoml_args(self, args: dict):
        str_args: str = ''
        for k, v in args.items():
            if isinstance(v, bool):
                str_args += f' --{k}'
            else:
                str_args += f' --{k}={v}'
        return str_args.lstrip()

    def _is_service_healthy(self, port: int, retries: int, backoff_time: int = 1) -> bool:
        """Checks healthz endpoint of BentoML model for life.

        Args:
            port (int): Port of the deployed model.
            retries (int): Number of retries before giving up.

        Returns:
            bool: Whether service is reachable or not.
        """
        logger.debug('Checking for service health.')
        logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
        session = requests.Session()

        # MonkeyPatching the backoff time
        def custom_backoff_time(self):
            return backoff_time

        Retry.get_backoff_time = custom_backoff_time
        retries = Retry(total=retries)
        session.mount('http://', HTTPAdapter(max_retries=retries))
        try:
            response = session.get(f'http://localhost:{port}/healthz', timeout=1)
            if response.status_code == 200:
                logger.debug('Service up and running.')
                return True
        except Exception:
            logger.debug('Health check unsuccessful.')
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
