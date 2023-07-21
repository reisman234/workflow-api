FROM python:3.8-alpine

# [Optional] If your pip requirements rarely change, uncomment this section to add them to the image.
COPY requirements.txt /tmp/pip-tmp/
RUN pip3 --disable-pip-version-check --no-cache-dir install \
    -r /tmp/pip-tmp/requirements.txt && rm -rf /tmp/pip-tmp

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends iputils-ping netcat

WORKDIR /opt/
COPY entrypoint.sh entrypoint.sh
COPY middlelayer ./middlelayer

ENV FASTAPI_ROOT_PATH=/

EXPOSE 8080

CMD ["/bin/sh", "entrypoint.sh"]
