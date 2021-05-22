import os
import re
import subprocess
import tempfile
import time

import libtmux
import yaml
from conda.cli.python_api import Commands, run_command
from fastapi import HTTPException, status

from app.base_deployment import Deployment
from app.models import Envs


class TmuxDeployment(Deployment):

    BENTOML_FLASK_SERVING_STR = 'Serving Flask app'
    BENTOML_GUNICORN_SERVING_STR = 'Booting worker'

    def __init__(self, model: str, version: str, env: Envs):
        super().__init__(model, version, env)
        model_clean = re.sub(r'\W+', '', self.model)
        self.env_name = f'{self.env}_{model_clean}'
        self.prefix = os.path.abspath(os.path.join('./envs', self.env_name))
        self.session_name = f'bentoml_{self.env}_{model_clean}'

    def deploy_model(self, port: int, workers: int):
        server = libtmux.Server()
        self._kill_session_if_exists(server)
        self._create_env_from_model()
        if self._is_port_in_use(port):
            self.logger.error(f'Port {port} is already in use.')
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY, detail=f'Port {port} is already in use.'
            )
        session = server.new_session(session_name=self.session_name)
        session.set_environment('model_name', self.model)
        session.set_environment('model_version', self.version)
        session.set_environment('model_env', self.env)
        session.set_environment('model_port', port)
        session.set_environment('model_workers', workers)
        pane = session.attached_pane
        pane.send_keys(f'conda activate {self.prefix}')
        if self.env == 'dev':
            pane.send_keys(f'bentoml serve --port {port} {self.model}:{self.version}')
        else:
            pane.send_keys(
                f'bentoml serve-gunicorn --port {port} --workers {workers} {self.model}:{self.version}'
            )

        self._wait_for_capture(pane, 10)
        self.logger.info(f'Deployed model in session: {self.session_name}')
        return super().deploy_model()

    def undeploy_model(self):
        server = libtmux.Server()
        self._kill_session_if_exists(server)
        self._delete_env_if_exists()
        self.logger.info(f'Undeployed model from session: {self.session_name}')
        return super().undeploy_model()

    @classmethod
    def get_running_models(self):
        server = libtmux.Server()
        sessions = server.list_sessions()
        sessions_fmt = []
        for session in sessions:
            name = session.get('session_name')
            if not name.startswith('bentoml_'):
                continue
            sessions_fmt.append(
                {
                    'model': session.show_environment('model_name'),
                    'version': session.show_environment('model_version'),
                    'env': session.show_environment('model_env'),
                    'port': session.show_environment('model_port'),
                    'workers': session.show_environment('model_workers'),
                }
            )
        logger = Deployment.init_logger()
        logger.debug(f'Running model sessions: {str(sessions_fmt)}')
        return sessions_fmt

    def _wait_for_capture(self, pane, timeout: int):
        activated_env = False
        started_server = False
        for _ in range(timeout * 2):
            time.sleep(0.5)
            outputs = pane.capture_pane()
            for line in outputs:
                activated_env = activated_env | (f'({self.env_name})' in line)
                started_server = (
                    started_server
                    | (self.BENTOML_FLASK_SERVING_STR in line)
                    | (self.BENTOML_GUNICORN_SERVING_STR in line)
                )
                if activated_env and started_server:
                    return
        str_output = '\n'.join(outputs)
        if activated_env is False:
            self.logger.error(f'Could not activate conda env.\n{str_output}')
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'Could not activate conda env.\n{str_output}',
            )
        if started_server is False:
            self.logger.error(f'Could not start server.\n{str_output}')
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f'Could not start server.\n{str_output}',
            )

    def _kill_session_if_exists(self, server):
        if server.has_session(self.session_name):
            session = server.find_where({"session_name": self.session_name})
            session.kill_session()
            self.logger.debug(f'Killed running session: {self.session_name}')

    def _delete_env_if_exists(self):
        envs = run_command(Commands.INFO, '--envs')
        if self.prefix in envs[0]:
            run_command(Commands.REMOVE, '--all', '--prefix', self.prefix)
            self.logger.debug(f'Removed conda env: {self.prefix}')

    def _create_env_from_model(self):
        bentoml_model = self.get_bentoml_model_by_version()
        bentoml_model_env = bentoml_model.env.to_dict()
        python_version = bentoml_model_env['python_version']
        pip_packages = bentoml_model_env['pip_packages']
        config = {
            'name': self.env_name,
            'channels': ['defaults'],
            'dependencies': [f'python={python_version}', 'pip', {'pip': pip_packages}],
        }
        with tempfile.TemporaryDirectory() as tmpdirname:
            env_yml = os.path.join(tmpdirname, 'environment.yml')
            with open(env_yml, 'w') as file:
                yaml.safe_dump(config, file)
            self._delete_env_if_exists()
            response = subprocess.run(
                args=f'bash -c "source activate root; conda env create --prefix {self.prefix} --file {env_yml}"',
                timeout=240,
                shell=True,
                stdout=subprocess.PIPE,
            )
            if response.returncode < 0:
                self.logger.error(f'Could not create conda env.\n{response.stderr.decode("utf-8")}')
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f'Could not create conda env.\n{response.stderr.decode("utf-8")}',
                )
            self.logger.debug(f'Created new conda env: {self.prefix}')
