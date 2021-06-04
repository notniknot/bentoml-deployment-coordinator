from enum import Enum
from typing import NewType

from pydantic import BaseModel


class RuntimeEnv(Enum):
    DOCKER = 'docker'
    TMUX = 'tmux'


class Stage(Enum):
    STAGING = 'staging'
    PRODUCTION = 'production'


RuntimeEnvType = NewType('Runtime', RuntimeEnv)
StageType = NewType('Stage', Stage)


class DeployModelInput(BaseModel):
    model: str
    version: str
    stage: StageType = Stage.PRODUCTION
    port: int = 5000
    workers: int = 1
    runtime_env: RuntimeEnvType = RuntimeEnv.TMUX


class UndeployModelInput(BaseModel):
    model: str
    stage: StageType = Stage.PRODUCTION
    runtime_env: RuntimeEnvType = RuntimeEnv.TMUX
