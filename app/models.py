from enum import Enum
from typing import NewType

from pydantic import BaseModel


class RuntimeEnv(Enum):
    DOCKER = 'docker'
    TMUX = 'tmux'


RuntimeEnvType = NewType('Runtime', RuntimeEnv)


class DeployModelInput(BaseModel):
    model: str
    version: str
    port: int = 5000
    workers: int = 1
    runtime_env: RuntimeEnvType = RuntimeEnv.TMUX


class UndeployModelInput(BaseModel):
    model: str
    version: str
    runtime_env: RuntimeEnvType = RuntimeEnv.TMUX
