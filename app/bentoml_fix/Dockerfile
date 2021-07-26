ARG BASE_IMAGE
FROM ${BASE_IMAGE}

COPY --chown=bentoml:bentoml marshal_server.py /opt/conda/lib/python3.8/site-packages/bentoml/server/marshal_server.py
COPY --chown=bentoml:bentoml marshal.py /opt/conda/lib/python3.8/site-packages/bentoml/marshal/marshal.py

WORKDIR $BUNDLE_PATH
CMD ["bentoml", "serve-gunicorn", "./"]