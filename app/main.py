import secrets
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from app.tmux_deployment import TmuxDeployment
from app.models import DeployModelInput, UndeployModelInput
from fastapi import Response

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


@app.post("/start", dependencies=[Depends(authenticate)])
async def start(model_content: DeployModelInput):
    tmux = TmuxDeployment(model_content.model, model_content.version, model_content.env)
    response = tmux.deploy_model(port=model_content.port)
    return Response(status_code=status.HTTP_200_OK, content=response)


@app.post("/stop", dependencies=[Depends(authenticate)])
async def stop(model_content: UndeployModelInput):
    tmux = TmuxDeployment(model_content.model, model_content.version, model_content.env)
    response = tmux.undeploy_model()
    return Response(status_code=status.HTTP_200_OK, content=response)


@app.get("/running")
async def running():
    return TmuxDeployment.get_running_models()
