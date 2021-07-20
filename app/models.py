from enum import Enum
from typing import Optional

from pydantic import BaseModel


class RuntimeEnv(Enum):
    DOCKER = 'docker'
    TMUX = 'tmux'


class Stage(Enum):
    NONE = 'None'
    STAGING = 'Staging'
    PRODUCTION = 'Production'
    ARCHIVED = 'Archived'


class DeployModelInput(BaseModel):
    name: str
    version: str
    stage: Stage
    old_stage: Optional[Stage] = Stage.NONE
    runtime_env: RuntimeEnv
    args: Optional[dict] = None
    batch_prediction: bool = False
    airflow: Optional[dict] = None


class UndeployModelInput(BaseModel):
    name: str
    version: str
    old_stage: Optional[Stage] = Stage.NONE
    runtime_env: RuntimeEnv
    batch_prediction: bool = False
    airflow: Optional[dict] = None


DEFAULT_ARGS = {'port': 5000}
