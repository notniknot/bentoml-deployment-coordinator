import logging
import secrets
from typing import Union

from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.responses import RedirectResponse

from app.airflow_deployment import AirflowDeployment
from app.docker_deployment import DockerDeployment
from app.models import DEFAULT_ARGS, DeployModelInput, RuntimeEnv, UndeployModelInput
from app.tmux_deployment import TmuxDeployment
from app.utils import init_logger

init_logger()
logger = logging.getLogger(f'coordinator.{__name__}')

app = FastAPI(
    title='BentoML Deployment Coordinizer',
    description='A webservice that provides endpoints to manage ML-Model-Deployments via BentoML in tmux-sessions or Docker containers',
    version='1.0',
)

security = HTTPBasic()


def authenticate(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    """Basic HTTP authentication.

    Args:
        credentials (HTTPBasicCredentials, optional): Containing username and password. Defaults to Depends(security).

    Raises:
        HTTPException: If credentials don't match.

    Returns:
        str: Username if authenticated successfully.
    """
    correct_username = secrets.compare_digest(credentials.username, "user")
    correct_password = secrets.compare_digest(credentials.password, "pw")
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


def get_runtime_env(runtime_env: RuntimeEnv) -> Union[DockerDeployment, TmuxDeployment]:
    """Choose appropriate runtime environment.

    Args:
        runtime_env (RuntimeEnv): Runtime environment enum

    Returns:
        Union[DockerDeployment, TmuxDeployment]: Either Docker- or Tmux-Class.
    """
    if runtime_env == RuntimeEnv.DOCKER:
        return DockerDeployment
    if runtime_env == RuntimeEnv.TMUX:
        return TmuxDeployment
    else:
        raise ValueError(f'Runtime env {runtime_env.value} not supported.')


@app.get("/", include_in_schema=False)
async def get_docs():
    """Redirect to SwaggerUI"""
    return RedirectResponse(url='/docs')


@app.post(
    "/start",
    dependencies=[Depends(authenticate)],
    name='Deploy a BentoML Model',
    tags=['Operations'],
)
async def start(model_content: DeployModelInput):
    runtime_env = get_runtime_env(model_content.runtime_env)
    runtime_env_instance = runtime_env(
        name=model_content.name, version=model_content.version, stage=model_content.stage
    )
    DEFAULT_ARGS.update(model_content.args or dict())
    response = runtime_env_instance.deploy_model(args=DEFAULT_ARGS)
    if model_content.batch_prediction is True and isinstance(model_content.airflow, dict):
        airflow = AirflowDeployment(
            airflow=model_content.airflow,
            name=model_content.name,
            version=model_content.version,
            suffix=response['suffix'],
            stage=model_content.stage,
        )
        airflow.undeploy_model(response['removed_containers'])
        airflow.deploy_model()
    return Response(
        status_code=status.HTTP_200_OK, content=f'Started {response["deployment_name"]}'
    )


@app.post(
    "/stop",
    dependencies=[Depends(authenticate)],
    name='Undeploy a BentoML Model',
    tags=['Operations'],
)
async def stop(model_content: UndeployModelInput):
    runtime_env = get_runtime_env(model_content.runtime_env)
    runtime_env_instance = runtime_env(name=model_content.name, version=model_content.version)
    stopped_containers = runtime_env_instance.undeploy_model()
    if model_content.batch_prediction is True and isinstance(model_content.airflow, dict):
        airflow = AirflowDeployment(
            airflow=model_content.airflow, name=model_content.name, version=model_content.version
        )
        airflow.undeploy_model(stopped_containers)
    return Response(status_code=status.HTTP_200_OK, content=f'Stopped {stopped_containers}')


@app.get(
    "/running",
    name='List all deployed Models',
    tags=['Information'],
)
async def running():
    return {
        'tmux': TmuxDeployment.get_running_models(),
        'docker': DockerDeployment.get_running_models(),
        'airflow': AirflowDeployment.get_running_models(),
    }
