import logging
import re
from pathlib import Path
from typing import List

import yaml

from app.base_deployment import Deployment
from app.models import Stage
from app.utils import _get_config

logger = logging.getLogger(f'coordinator.{__name__}')


class PrometheusDeployment(Deployment):
    def __init__(
        self,
        name: str,
        version: str = None,
        suffix: str = None,
        stage: Stage = Stage.NONE,
    ):
        """Create instance of base deployment technique.

        Args:
            model (str): Name of the model.
            version (str): Version of the model.
            stage (Stage): New stage of the model.
        """
        super().__init__(name=name, stage=stage, version=version, suffix=suffix)
        self.targets_path, self.targets = PrometheusDeployment.load_targets()

    @classmethod
    def load_targets(cls):
        targets_path = Path(_get_config(('prometheus', 'targets')))
        with open(targets_path, 'r') as file:
            targets = yaml.safe_load(file)
        return targets_path, targets

    def deploy_model(self, port):
        """Abstract method to deploy model."""
        logger.info(f'Deploying {self.deployment_name} to Prometheus.')

        self.remove_target(by=['suffix', 'stage'])

        self.targets.append(
            {
                'targets': [f'srv-esa01.cosmos.local:{port}'],
                'labels': {
                    'deployment_name': self.deployment_name,
                    'model': self.name,
                    'version': self.version,
                    'stage': self.stage,
                },
            }
        )

        with open(self.targets_path, 'w') as file:
            yaml.safe_dump(self.targets, file)

    def undeploy_model(self, removed_containers: list):
        """Abstract method to undeploy model."""
        for removed_container in removed_containers:
            self.remove_target(by=['name'], name=removed_container.name)
        with open(self.targets_path, 'w') as file:
            yaml.safe_dump(self.targets, file)

    def remove_target(self, by: List[str], name: str = None):
        def remove_by(by_regex: re.Pattern = None, by_name: str = None):
            to_delete = []
            for target in self.targets:
                if not isinstance(target, dict) or 'deployment_name' not in target.get(
                    'labels', {}
                ):
                    continue
                if by_regex is not None and by_regex.match(target['labels']['deployment_name']):
                    to_delete.append(target)
                if by_name is not None and target['labels']['deployment_name'] == name:
                    to_delete.append(target)
            return to_delete

        to_delete = []
        if 'suffix' in by:
            regex_by_suffix = re.compile(
                r'^{}_{}_\w+_{}$'.format(PrometheusDeployment.prefix, self.name_clean, self.suffix)
            )
            to_delete += remove_by(by_regex=regex_by_suffix)
        if 'stage' in by:
            regex_by_stage = re.compile(
                r'^{}_{}_{}_\w+$'.format(
                    PrometheusDeployment.prefix, self.name_clean, self.stage_clean
                )
            )
            to_delete += remove_by(by_regex=regex_by_stage)
        if 'name' in by:
            if name is None:
                raise ValueError('Parameter "name" cannot be None')
            to_delete += remove_by(by_name=name)
        self.targets = [target for target in self.targets if target not in to_delete]

    @classmethod
    def get_running_models(cls):
        """Abstract method to get running models."""
        _, targets = cls.load_targets()
        target_list = [
            target
            for target in targets
            if isinstance(target, dict) and 'deployment_name' in target.get('labels', {})
        ]
        return target_list
