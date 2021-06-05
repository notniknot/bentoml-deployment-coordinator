import logging
import os
import socket
import time

import requests
from bentoml.yatai.client import get_yatai_client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.models import StageType
from app.utils import get_config


class Deployment:
    def __init__(self, model: str, stage: StageType, version: str = ''):
        os.environ['BENTOML_DO_NOT_TRACK'] = 'True'
        self.logger = self.init_logger()
        self.logger.info(f'Initializing {type(self).__name__}: {model}:{version}')
        self.model = model
        self.version = version
        self.stage = stage.value
        for k, v in get_config('yatai').items():
            os.environ[k] = v

    def get_bentoml_model_by_version(self):
        yatai_client = get_yatai_client()
        return yatai_client.repository.load(f'{self.model}:{self.version}')

    def deploy_model(self):
        return 'Successfully deployed model'

    def undeploy_model(self):
        return 'Successfully undeployed model'

    @classmethod
    def init_logger(self):
        root_handler = logging.StreamHandler()
        root_handler.setLevel(logging.DEBUG)
        logging.basicConfig(
            format='[%(asctime)s] %(levelname)s  %(name)s: %(message)s',
            level=logging.WARNING,
            handlers=[root_handler],
            force=True,
        )
        # Suppress sqlalchemy echo
        sqlalchemy = logging.getLogger('sqlalchemy.engine.base.Engine')
        sh = logging.StreamHandler()
        sh.setLevel(logging.WARNING)
        sqlalchemy.handlers = [sh]
        sqlalchemy.propagate = False

        logger = logging.getLogger('coordinator')
        logger.setLevel(logging.DEBUG)
        return logger

    @classmethod
    def get_running_models(self):
        return list()

    def _is_service_healthy(self, port: int, retries: int):
        self.logger.debug('Checking for service health.')
        logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
        session = requests.Session()
        retries = Retry(total=retries, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        try:
            response = session.get(f'http://127.0.0.1:{port}/healthz', timeout=1)
            if response.status_code == 200:
                self.logger.debug('Service up and running.')
                return True
        except Exception:
            self.logger.debug('Health check unsuccessful.')
            return False

    def _is_port_in_use(self, port: int, retry: int = 3):
        for _ in range(retry):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if s.connect_ex(('localhost', port)) != 0:
                    return False
            time.sleep(1)
        return True
