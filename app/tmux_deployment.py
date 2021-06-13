import os
import random
import re
import string
import subprocess
import tempfile
from typing import List, Literal

import libtmux
import yaml
from conda.cli.python_api import Commands, run_command
from fastapi import HTTPException, status
from libtmux.exc import LibTmuxException

from app.base_deployment import Deployment
from app.models import Stage, StageType
from app.utils import _distinct, _get_config


class TmuxDeployment(Deployment):

    BENTOML_FLASK_SERVING_STR = 'Serving Flask app'
    BENTOML_GUNICORN_SERVING_STR = 'Booting worker'

    def __init__(self, model: str, version: str, stage: StageType = Stage.NONE):
        """[summary]

        Args:
            model (str): [description]
            version (str): [description]
            stage (StageType, optional): [description]. Defaults to Stage.NONE.
        """
        super().__init__(model=model, version=version, stage=stage)
        model_clean = re.sub(r'\W+', '', self.model).lower()
        stage_clean = re.sub(r'\W+', '', self.stage).lower()
        random_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        self.env_name = f'bentoml_{model_clean}_{stage_clean}_{random_string}'
        self.env_name_general = f'bentoml_{model_clean}_{stage_clean}'
        self.prefix = os.path.abspath(os.path.join('./envs', self.env_name))
        self.prefix_general = os.path.abspath(os.path.join('./envs', self.env_name_general))
        self.session_name = f'bentoml_{model_clean}_{stage_clean}_{random_string}'
        self.session_name_general = f'bentoml_{model_clean}_{stage_clean}'

    def deploy_model(self, port: int, workers: int):
        """[summary]

        Args:
            port (int): [description]
            workers (int): [description]

        Raises:
            HTTPException: [description]
            ex: [description]

        Returns:
            [type]: [description]
        """
        server = libtmux.Server()
        self._create_env_from_model()
        # ? Nicht nur Version, sondern auch Stage???
        stopped_sessions = self._stop_model_server(find_by=['version'], kill_session=False)
        if self._is_port_in_use(port, 4):
            self.logger.error(f'Port {port} is already in use. Cleaning up...')
            self._delete_env_if_exists(specific_prefix=self.prefix)
            self._start_model_server(server, existing_sessions=stopped_sessions, raise_error=False)
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY, detail=f'Port {port} is already in use.'
            )
        try:
            self._start_model_server(server, workers=workers, port=port)
            stopped_sessions = self._stop_model_server(
                find_by=['stage', 'version'], kill_session=True, exclude=self.session_name
            )
            for session in stopped_sessions:
                self._delete_env_if_exists(
                    exclude=self.prefix, specific_prefix=session['used_conda_prefix']
                )
            self.logger.info(f'Deployed model in session: {self.session_name}')
            # ToDo: 'Unrecognized response type; displaying content as text.'
            return 'Deployed model'
        except HTTPException as ex:
            self.logger.info('Model could not be deployed. Starting old model server if existing.')
            self._start_model_server(server, workers, port, existing_session=True)
            raise ex

    def undeploy_model(self):
        """[summary]

        Raises:
            HTTPException: [description]

        Returns:
            [type]: [description]
        """
        stopped_sessions = self._stop_model_server(find_by=['version'], kill_session=True)
        for stopped_session in stopped_sessions:
            self._delete_env_if_exists(specific_prefix=stopped_session['used_conda_prefix'])
        if len(stopped_sessions) > 0:
            self.logger.info(f'Undeployed model (tmux): {self.model}, {self.version}')
            return 'Successfully undeployed model'
        else:
            self.logger.info(f'Model could not be undeployed: {self.model}, {self.version}')
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f'Model could not be undeployed: {self.model}, {self.version}',
            )

    @classmethod
    def get_running_models(
        self,
        name: str = None,
        version: str = None,
        session_name_start: str = None,
        return_only_sessions: bool = False,
    ) -> List[dict]:
        """[summary]

        Args:
            name (str, optional): [description]. Defaults to None.
            version (str, optional): [description]. Defaults to None.
            session_name_start (str, optional): [description]. Defaults to None.
            return_only_sessions (bool, optional): [description]. Defaults to False.

        Returns:
            List[dict]: [description]
        """
        logger = self.init_logger()
        server = libtmux.Server()
        try:
            sessions = server.list_sessions()
        except LibTmuxException:
            logger.info('No running tmux-Sessions found.')
            return list()
        sessions_fmt = []
        for session in sessions:
            session_name = session.get('session_name')
            if not session_name.startswith('bentoml_'):
                continue
            if session_name_start is not None and not session_name.startswith(session_name_start):
                continue
            if name is not None and session.show_environment('model_name') != name:
                continue
            if version is not None and session.show_environment('model_version') != version:
                continue
            labels = ['name', 'version', 'stage', 'port', 'workers']
            if not all(session.show_environment(f'model_{label}') for label in labels):
                continue
            if return_only_sessions is True:
                sessions_fmt.append(session)
            else:
                sessions_fmt.append(
                    {label: session.show_environment(f'model_{label}') for label in labels}
                )
        if return_only_sessions is False:
            logger.debug(f'Running model sessions: {str(sessions_fmt)}')
        return sessions_fmt

    def _start_model_server(
        self,
        server: libtmux.Server,
        existing_sessions: List[libtmux.Session] = None,
        port: int = None,
        workers: int = None,
        raise_error: bool = True,
    ):
        """[summary]

        Args:
            server (libtmux.Server): [description]
            existing_sessions (List[libtmux.Session], optional): [description]. Defaults to None.
            port (int, optional): [description]. Defaults to None.
            workers (int, optional): [description]. Defaults to None.
            raise_error (bool, optional): [description]. Defaults to True.

        Raises:
            LibTmuxException: [description]
        """
        self.logger.debug(f'Starting model server, existing_session={existing_sessions}.')

        if isinstance(existing_sessions, list):
            for existing_session in existing_sessions:
                self._launch_gunicorn_in_session(existing_session, raise_error)
                self.logger.debug(f'Restarted stopped Session: {existing_session.name}')
            if len(existing_sessions) == 0:
                self.logger.debug('Old Sessions could not be found.')
                if raise_error:
                    raise LibTmuxException('Old Sessions could not be found.')
            return

        session = server.new_session(session_name=self.session_name)
        for k, v in _get_config('yatai').items():
            session.set_environment(k, v)
        for k, v in _get_config('env_vars').items():
            session.set_environment(k, v)
        session.set_environment('model_name', self.model)
        session.set_environment('model_version', self.version)
        session.set_environment('model_stage', self.stage)
        session.set_environment('model_port', port)
        session.set_environment('model_workers', workers)
        session.set_environment('model_conda_prefix', self.prefix)
        self._launch_gunicorn_in_session(session, raise_error)
        self.logger.debug(f'Started model server: {session.name}.')

    def _launch_gunicorn_in_session(self, session: libtmux.Session, raise_error: bool):
        """[summary]

        Args:
            session (libtmux.Session): [description]
            raise_error (bool): [description]

        Raises:
            HTTPException: [description]
        """
        pane = session.attached_pane
        pane.send_keys(f'conda activate {self.prefix}')
        used_model = session.show_environment('model_name')
        used_version = session.show_environment('model_version')
        used_port = session.show_environment('model_port')
        used_workers = session.show_environment('model_workers')
        pane.send_keys(
            f'bentoml serve-gunicorn --port {used_port} --workers {used_workers} {used_model}:{used_version}'
        )
        if not self._is_service_healthy(used_port, 7):
            detail = '\n'.join(pane.capture_pane())
            pane.send_keys('C-c', enter=False, suppress_history=False)
            session.kill_session()
            self.logger.info(f'Could not deploy service: {detail}')
            if raise_error:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f'Could not deploy service.\n{detail}',
                )

    def _stop_model_server(
        self, find_by: List[Literal['version', 'stage']], kill_session: bool, exclude: str = ''
    ) -> List[dict]:
        """[summary]

        Args:
            find_by (List[Literal[): [description]
            kill_session (bool): [description]
            exclude (str, optional): [description]. Defaults to ''.

        Returns:
            List[dict]: [description]
        """
        self.logger.debug(f'Stopping possible running model server, kill_session={kill_session}.')

        sessions = []
        if 'version' in find_by:
            sessions += self.get_running_models(
                name=self.model, version=self.version, return_only_sessions=True
            )
        if 'stage' in find_by:
            sessions += self.get_running_models(
                session_name_start=self.session_name_general, return_only_sessions=True
            )

        stopped_sessions = []
        for session in _distinct(sessions, 'name'):
            if session.name == exclude:
                continue
            session.attached_pane.send_keys('C-c', enter=False, suppress_history=False)
            stopped_sessions.append(
                {
                    'used_model': session.show_environment('model_name'),
                    'used_version': session.show_environment('model_version'),
                    'used_port': session.show_environment('model_port'),
                    'used_workers': session.show_environment('model_workers'),
                    'used_conda_prefix': session.show_environment('model_conda_prefix'),
                }
            )
            self.logger.debug(f'Stopped model server: {session.name}')
            if kill_session:
                self.logger.debug(f'Killing (old) session: {session.name}')
                session.kill_session()
        if len(stopped_sessions) == 0:
            self.logger.debug('No running sessions stopped.')
        return stopped_sessions

    def _delete_env_if_exists(self, exclude: str = '', specific_prefix: str = None) -> bool:
        """[summary]

        Args:
            exclude (str, optional): [description]. Defaults to ''.
            specific_prefix (str, optional): [description]. Defaults to None.

        Returns:
            bool: [description]
        """
        self.logger.error(f'Deleting conda environments: {self.prefix_general}')
        envs = run_command(Commands.INFO, '--envs')[0].split()
        found_envs = []
        if specific_prefix is not None and specific_prefix in envs:
            found_envs = [specific_prefix]
        else:
            found_envs = [
                env
                for env in envs
                if self.prefix_general in env and (not exclude or exclude not in env)
            ]
        for env in found_envs:
            run_command(Commands.REMOVE, '--all', '--prefix', env)
            self.logger.debug(f'Removed conda env: {env}')
        if len(found_envs) == 0:
            self.logger.debug(f'No conda environments found: {self.prefix_general}')
            return False
        else:
            return True

    def _create_env_from_model(self):
        """[summary]

        Raises:
            HTTPException: [description]
        """
        self.logger.debug(f'Creating new conda environment: {self.prefix}')
        _, bentoml_model = self.get_bentoml_model_by_version()
        bentoml_model_env = bentoml_model.bento_service_metadata.env
        python_version = bentoml_model_env.python_version
        pip_packages = list(bentoml_model_env.pip_packages)
        pip_packages = list(set(pip_packages + ['psycopg2-binary', 'boto3']))
        config = {
            'name': self.env_name,
            'channels': ['defaults'],
            'dependencies': [f'python={python_version}', 'pip', {'pip': pip_packages}],
        }
        with tempfile.TemporaryDirectory() as tmpdirname:
            env_yml = os.path.join(tmpdirname, 'environment.yml')
            with open(env_yml, 'w') as file:
                yaml.safe_dump(config, file)
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
            self.logger.debug(f'Created new conda environment: {self.prefix}')
