from enum import Enum
from typing import NewType

from pydantic import BaseModel


class Namespace(Enum):
    DEV = 'dev'
    PROD = 'prod'


class RuntimeEnv(Enum):
    DOCKER = 'docker'
    TMUX = 'tmux'


NamespaceType = NewType('Env', Namespace)
RuntimeEnvType = NewType('Runtime', RuntimeEnv)


class DeployModelInput(BaseModel):
    model: str
    version: str
    namespace: NamespaceType
    port: int = 5000
    workers: int = 1
    runtime_env: RuntimeEnvType = RuntimeEnv.TMUX


class UndeployModelInput(BaseModel):
    model: str
    version: str
    namespace: NamespaceType
    runtime_env: RuntimeEnvType = RuntimeEnv.TMUX
