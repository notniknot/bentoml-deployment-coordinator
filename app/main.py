import secrets
from typing import Union

from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.docker_deployment import DockerDeployment
from app.models import DeployModelInput, RuntimeEnv, RuntimeEnvType, UndeployModelInput
from app.tmux_deployment import TmuxDeployment

app = FastAPI()

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


@app.post("/start", dependencies=[Depends(authenticate)])
async def start(model_content: DeployModelInput):
    runtime_env = get_runtime_env(model_content.runtime_env)
    runtime_env_instance = runtime_env(
        model_content.model, model_content.version, model_content.namespace
    )
    response = runtime_env_instance.deploy_model(
        port=model_content.port, workers=model_content.workers
    )
    return Response(status_code=status.HTTP_200_OK, content=response)


@app.post("/stop", dependencies=[Depends(authenticate)])
async def stop(model_content: UndeployModelInput):
    runtime_env = get_runtime_env(model_content.runtime_env)
    runtime_env_instance = runtime_env(
        model_content.model, model_content.version, model_content.namespace
    )
    response = runtime_env_instance.undeploy_model()
    return Response(status_code=status.HTTP_200_OK, content=response)


@app.get("/running")
async def running():
    return {
        'tmux': TmuxDeployment.get_running_models(),
        'docker': DockerDeployment.get_running_models(),
    }


# ToDo: Log to MLflow/Page?
