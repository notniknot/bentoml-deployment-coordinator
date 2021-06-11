from enum import Enum
from typing import NewType

from pydantic import BaseModel


class RuntimeEnv(Enum):
    DOCKER = 'docker'
    TMUX = 'tmux'


class Stage(Enum):
    NONE = 'None'
    STAGING = 'Staging'
    PRODUCTION = 'Production'
    ARCHIVED = 'Archived'


RuntimeEnvType = NewType('Runtime', RuntimeEnv)
StageType = NewType('Stage', Stage)


class DeployModelInput(BaseModel):
    model: str
    version: str
    stage: StageType = Stage.PRODUCTION
    old_stage: StageType = Stage.NONE
    port: int = 5000
    workers: int = 1
    runtime_env: RuntimeEnvType = RuntimeEnv.TMUX


class UndeployModelInput(BaseModel):
    model: str
    version: str
    old_stage: StageType = Stage.PRODUCTION
    runtime_env: RuntimeEnvType = RuntimeEnv.TMUX
