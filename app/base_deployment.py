import logging
import socket

from bentoml.yatai.client import get_yatai_client


class Deployment:
    def __init__(self, model: str, version: str, env: str):
        self.logger = Deployment.init_logger()
        self.logger.info(f'Initializing {type(self).__name__}: {model}:{version} ({env})')
        self.model = model
        self.version = version
        self.env = env.value

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
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        return logger

    @classmethod
    def get_running_models(self):
        pass

    def _is_port_in_use(self, port: int):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', port)) == 0
