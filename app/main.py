import secrets
from typing import Union

from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.responses import RedirectResponse

from app.docker_deployment import DockerDeployment
from app.models import DeployModelInput, RuntimeEnv, RuntimeEnvType, UndeployModelInput
from app.tmux_deployment import TmuxDeployment

app = FastAPI(
    title='BentoML Deployment Coordinizer',
    description='A webservice that provides endpoints to manage ML-Model-Deployments via BentoML in tmux-sessions or Docker containers',
    version='1.0',
)

security = HTTPBasic()


def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, "user")
    correct_password = secrets.compare_digest(credentials.password, "pw")
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


def get_runtime_env(runtime_env: RuntimeEnvType) -> Union[DockerDeployment, TmuxDeployment]:
    if runtime_env == RuntimeEnv.DOCKER:
        return DockerDeployment
    if runtime_env == RuntimeEnv.TMUX:
        return TmuxDeployment


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
        model=model_content.model, stage=model_content.stage, version=model_content.version
    )
    response = runtime_env_instance.deploy_model(
        port=model_content.port, workers=model_content.workers
    )
    return Response(status_code=status.HTTP_200_OK, content=response)


@app.post(
    "/stop",
    dependencies=[Depends(authenticate)],
    name='Undeploy a BentoML Model',
    tags=['Operations'],
)
async def stop(model_content: UndeployModelInput):
    runtime_env = get_runtime_env(model_content.runtime_env)
    runtime_env_instance = runtime_env(model=model_content.model, stage=model_content.stage)
    response = runtime_env_instance.undeploy_model()
    return Response(status_code=status.HTTP_200_OK, content=response)


@app.get(
    "/running",
    name='List all deployed Models',
    tags=['Information'],
)
async def running():
    return {
        'tmux': TmuxDeployment.get_running_models(),
        'docker': DockerDeployment.get_running_models(),
    }
