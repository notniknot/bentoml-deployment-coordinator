from app.base_deployment import Deployment


class DockerDeployment(Deployment):
    def deploy_model(self):
        return super().deploy_model()

    def undeploy_model(self):
        return super().undeploy_model()
