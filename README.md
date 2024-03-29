<h1 align="center">
	<img
		width="400"
		alt="bentoml-deployment-coordinator icon"
		src="docs/bentoml-deployment-coordinator.png"
    >
</h1>

<h3 align="center">
	A service to remotely operationalize and manage models from a <a href="https://docs.bentoml.org/en/latest/" target="_blank">BentoML</a> Repo in Docker Containers or tmux Sessions
</h3>

<p align="center">
    <img src="https://img.shields.io/badge/language-python-green">
    <img src="https://img.shields.io/badge/codestyle-black-black">
    <img src="https://img.shields.io/github/last-commit/notniknot/bentoml-deployment-coordinator">
</p>

<p align="center">
  <a href="#addressed-issues">Addressed Issues</a> •
  <a href="#target-group">Target Group</a> •
  <a href="#setup">Setup</a> •
  <a href="#todos">ToDos</a>
</p>


# Addressed Issues
This repo coordinates the deployment of ML-Models via BentoML adding the following aspects:
- Start the deployment with custom parameters
- Check if the deployment was successful
- Automatically retire old versions of the same model
- Rollback if deployment was unsuccessful
- Adding the service to Prometheus
- Adding Airflow DAGs for Batch Prediction


# Target Group
This repo is for engineers/data scientists who encountered the same problems when using BentoML in an end-to-end-workflow.


# Setup
## Installation
- Make sure either Docker is installed and user has Docker rights or tmux is installed
- Airflow and Prometheus are optional
- Create conda env from environment.yml

## Running
`gunicorn app.main:app -w 4 -k uvicorn.workers.UvicornWorker -t 320 -b 0.0.0.0:8000`

# ToDos
- Logic to check if tmux/docker is installed
