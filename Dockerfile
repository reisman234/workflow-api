FROM python:3.8-alpine

# [Optional] If your pip requirements rarely change, uncomment this section to add them to the image.
COPY requirements.txt /tmp/pip-tmp/
RUN pip3 --disable-pip-version-check --no-cache-dir install \
    -r /tmp/pip-tmp/requirements.txt && rm -rf /tmp/pip-tmp

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends iputils-ping netcat

WORKDIR /opt/k8s-api
COPY __init__.py __init__.py
COPY middlelayer middlelayer

EXPOSE 8080

CMD uvicorn middlelayer.service_api:service_api --host=0.0.0.0 --port=8080 --root-path=/uuid/workflow-api
