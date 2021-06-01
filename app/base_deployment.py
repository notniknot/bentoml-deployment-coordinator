import logging
import os
import socket

from bentoml.yatai.client import get_yatai_client

from app.utils import get_config


class Deployment:
    def __init__(self, model: str, version: str):
        os.environ['BENTOML_DO_NOT_TRACK'] = 'True'
        self.logger = self.init_logger()
        self.logger.info(f'Initializing {type(self).__name__}: {model}:{version}')
        self.model = model
        self.version = version
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
        logging.basicConfig(format='[%(asctime)s] %(levelname)s  %(name)s: %(message)s')
        sql = logging.getLogger('sqlalchemy')
        sql.setLevel(logging.ERROR)
        sql.disabled = True
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        return logger

    @classmethod
    def get_running_models(self):
        return list()

    def _is_port_in_use(self, port: int):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', port)) == 0
