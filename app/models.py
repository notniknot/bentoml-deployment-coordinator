from enum import Enum
from typing import NewType

from pydantic import BaseModel


class Env(Enum):
    DEV = 'dev'
    PROD = 'prod'


Envs = NewType('Env', Env)


class DeployModelInput(BaseModel):
    model: str
    version: str
    env: Envs
    port: int = 5000
    workers: int = 1


class UndeployModelInput(BaseModel):
    model: str
    version: str
    env: Envs
