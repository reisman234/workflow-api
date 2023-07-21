#!/bin/sh

# source .venv/bin/activate

uvicorn middlelayer.service_api:service_api --reload --host=0.0.0.0 --port=8080 --root-path=${FASTAPI_ROOT_PATH}
