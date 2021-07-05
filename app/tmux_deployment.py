import json
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
from app.models import Stage
from app.utils import _distinct, _get_config


class TmuxDeployment(Deployment):

    BENTOML_FLASK_SERVING_STR = 'Serving Flask app'
    BENTOML_GUNICORN_SERVING_STR = 'Booting worker'

    def __init__(self, name: str, version: str, stage: Stage = Stage.NONE):
        """Create instance of tmux deployment technique.

        Args:
            model (str): Name of the model.
            version (str, optional): Version of the model. Defaults to ''.
            stage (Stage, optional): New stage of the model. Defaults to Stage.NONE.
        """
        super().__init__(name=name, version=version, stage=stage)
        name_clean = re.sub(r'\W+', '', self.name).lower()
        stage_clean = re.sub(r'\W+', '', self.stage).lower()
        random_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        self.env_name = f'bentoml_{name_clean}_{stage_clean}_{random_string}'
        self.env_name_general = f'bentoml_{name_clean}_{stage_clean}'
        self.prefix = os.path.abspath(os.path.join('./envs', self.env_name))
        self.prefix_general = os.path.abspath(os.path.join('./envs', self.env_name_general))
        self.session_name = f'bentoml_{name_clean}_{stage_clean}_{random_string}'
        self.session_name_general = f'bentoml_{name_clean}_{stage_clean}'

    def deploy_model(self, args: dict):
        """Deploy model in tmux session.

        Args:
            args (dict): Dictionary containing all the  arguments for the bentoml call.

        Raises:
            HTTPException: If port is already in use.
        """
        server = libtmux.Server()
        self._create_env_from_model()
        # ? Nicht nur Version, sondern auch Stage???
        stopped_sessions = self._stop_model_server(find_by=['version'], kill_session=False)
        port = args['port']
        if self._is_port_in_use(port, 4):
            self.logger.error(f'Port {port} is already in use. Cleaning up...')
            self._delete_env_if_exists(specific_prefix=self.prefix)
            self._start_model_server(
                server, args, existing_sessions=stopped_sessions, raise_error=False
            )
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY, detail=f'Port {port} is already in use.'
            )
        try:
            self._start_model_server(server, args)
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
            self._start_model_server(
                server, args, existing_sessions=stopped_sessions, raise_error=False
            )
            raise ex

    def undeploy_model(self):
        """Undeploy model from tmux sesison.

        Raises:
            HTTPException: If tmux session could not be stopped.
        """
        stopped_sessions = self._stop_model_server(find_by=['version'], kill_session=True)
        for stopped_session in stopped_sessions:
            self._delete_env_if_exists(specific_prefix=stopped_session['used_conda_prefix'])
        if len(stopped_sessions) > 0:
            self.logger.info(f'Undeployed model (tmux): {self.name}, {self.version}')
            return 'Successfully undeployed model'
        else:
            self.logger.info(f'Model could not be undeployed: {self.name}, {self.version}')
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f'Model could not be undeployed: {self.name}, {self.version}',
            )

    @classmethod
    def get_running_models(
        self,
        name: str = None,
        version: str = None,
        session_name_start: str = None,
        return_only_sessions: bool = False,
    ) -> List[dict]:
        """Get running models in tmux sessions.

        Args:
            name (str, optional): Name of the model. Defaults to None.
            version (str, optional): Version of the model. Defaults to None.
            session_name_start (str, optional): String the session name starts with. Defaults to None.
            return_only_sessions (bool, optional): If session objects should be returned. Defaults to False.

        Returns:
            List[dict]: Information about running models.
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
        args: dict,
        existing_sessions: List[libtmux.Session] = None,
        raise_error: bool = True,
    ):
        """Create session and set environment variables.

        Args:
            server (libtmux.Server): libtmux Server object.
            existing_sessions (List[libtmux.Session], optional): List of existing models that should be restarted. Defaults to None.
            port (int, optional): Port for Gunicorn to use. Defaults to None.
            workers (int, optional): Number of workers to spawn. Defaults to None.
            raise_error (bool, optional): If errors should be raised. Defaults to True.

        Raises:
            LibTmuxException: If old sessions could not be found to restart Gunicorn.
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
        for k, v in _get_config('env_vars').items():
            session.set_environment(k, v)
        session.set_environment('model_name', self.name)
        session.set_environment('model_version', self.version)
        session.set_environment('model_stage', self.stage)
        session.set_environment('model_conda_prefix', self.prefix)
        session.set_environment('args', json.dumps(args))
        self._launch_gunicorn_in_session(session, raise_error)
        self.logger.debug(f'Started model server: {session.name}.')

    def _launch_gunicorn_in_session(self, session: libtmux.Session, raise_error: bool):
        """Launch Gunicorn in tmux session.

        Args:
            session (libtmux.Session): Session object.
            raise_error (bool): If errors should be raised.

        Raises:
            HTTPException: If model health check if unsuccessful.
        """
        pane = session.attached_pane
        pane.send_keys(f'conda activate {self.prefix}')
        used_model = session.show_environment('model_name')
        used_version = session.show_environment('model_version')
        used_args = session.show_environment('args')
        pane.send_keys(
            f'bentoml serve-gunicorn {self.get_bentoml_args(json.loads(used_args))} {used_model}:{used_version}'
        )
        used_port = int(json.loads(used_args)['port'])
        if not self._is_service_healthy(used_port, 20):
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
        """Find and stop model instances.

        Args:
            find_by (List[Literal[): Search sessions by 'version' and/or 'stage'.
            kill_session (bool): If the session should be killed.
            exclude (str, optional): Exclude session by name from search. Defaults to ''.

        Returns:
            List[dict]: List of stopped sessions.
        """
        self.logger.debug(f'Stopping possible running model server, kill_session={kill_session}.')

        sessions = []
        if 'version' in find_by:
            sessions += self.get_running_models(
                name=self.name, version=self.version, return_only_sessions=True
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
                    'used_conda_prefix': session.show_environment('model_conda_prefix'),
                    'used_args': session.show_environment('args'),
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
        """Check if conda enviornment exists and delete it.

        Args:
            exclude (str, optional): Exclude conda environment from search. Defaults to ''.
            specific_prefix (str, optional): Seach for specific conda prefix. Defaults to None.

        Returns:
            bool: Whether any conda enviornments could be deleted or not.
        """
        self.logger.debug(f'Deleting conda environments: {self.prefix_general}')
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
        """Set up a conda environment by the BentoML ProtoBuffer information.

        Raises:
            HTTPException: If conda environment could not be created.
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
