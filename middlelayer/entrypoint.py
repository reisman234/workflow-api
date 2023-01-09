from pydantic import BaseModel
import os
import datetime
import requests
import logging

from configparser import ConfigParser

from threading import Thread
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s) %(message)s',)

main_cfg = ConfigParser()
main_cfg.read("config/middlelayer.conf")

k8s_client_endpoint = main_cfg.get(section="k8s_client", option="endpoint")

edc_x_api_key = main_cfg.get(section="edc", option="x_api_key")

minio_cfg = main_cfg["minio"]
app = FastAPI()

RESOURCE_BASE = os.getenv("RESOURCE_BASE", "/opt/resources")
RESULT_BUCKET = "gx4ki-demo"


@app.get("/demo/{case}")
async def getDoDemo(case):

    reqUrl = f"{k8s_client_endpoint}/demo/"

    post_files = {
        "env_file": open(f"{RESOURCE_BASE}/{case}.env", "rb"),
        "config_data": open(f"{RESOURCE_BASE}/{case}-job-conf.env", "rb"),
    }
    headersList = {
        "Accept": "*/*",
        "User-Agent": "Thunder Client (https://www.thunderclient.com)"
    }

    payload = ""

    print("send request")
    response = requests.request(
        "GET", reqUrl, data=payload, files=post_files, headers=headersList, stream=True)
    print(response.headers)
    return StreamingResponse(response.iter_content(4096), headers=response.headers, status_code=response.status_code)


class EdcRequest(BaseModel):
    assetId: str
    transferProcessId: str
    callbackAddress: str
    resourceDefinitionId: str
    policy: dict
    # price: float
    # tax: Union[float, None] = None


def task(edcRequest: EdcRequest, demo_task):

    reqUrl = f"{k8s_client_endpoint}/demo/"

    post_files = {
        "env_file": open(f"{RESOURCE_BASE}/{demo_task}.env", "rb"),
        "config_data": open(f"{RESOURCE_BASE}/{demo_task}-job-conf.env", "rb"),
    }
    headersList = {
        "Accept": "*/*",
        "User-Agent": "Thunder Client (https://www.thunderclient.com)"
    }

    payload = ""

    # make request against imla-k8s-client
    # ... wait for job_id and job_results
    response = requests.request(
        "GET", reqUrl, data=payload, files=post_files, headers=headersList)

    job_result = response.json()
    logging.debug(response.headers)
    logging.debug(job_result)

    #
    resource_name = f"{job_result['job_id']}/{job_result['job_results'][0]}"
    data = {
        "edctype": "dataspaceconnector:provisioner-callback-request",
        "resourceDefinitionId": edcRequest.resourceDefinitionId,
        "assetId": edcRequest.assetId,
        "resourceName": resource_name,
        "contentDataAddress": {
            "properties": {
                "type": "AmazonS3",
                "region": "local",
                "keyName": "sample",  # keyName ignored
                "bucketName": RESULT_BUCKET,
                "accessKeyId": minio_cfg.get('access_key'),
                "secretAccessKey": minio_cfg.get('secret_key'),
                "endpointOverride": "http://" + minio_cfg.get('endpoint')
            }
        },
        "apiKeyJwt": "unused",
        "hasToken": False
    }
    completeUrl = f"{edcRequest.callbackAddress}/{edcRequest.transferProcessId}/provision"

    try:
        logging.debug(f"callback to {completeUrl}")
        resp = requests.post(url=completeUrl, json=data,
                             headers={"x-api-key": edc_x_api_key})
    except ConnectionError as e:
        print(f"ConnectionError: {completeUrl}")
        status_code = 400
    finally:
        logging.debug("work finished")


@app.post("/provision/")
async def getFirst(request: Request, edcRequest: EdcRequest, demo_task: str = "test"):
    logging.debug(f"header: {request.headers}")
    logging.debug(f"body {await request.body()}")
    logging.debug("create and start thread")
    logging.debug(f"do task: {demo_task}")
    t = Thread(target=task,
               kwargs={
                   "demo_task": demo_task,
                   "edcRequest": edcRequest}
               )
    t.start()
    logging.debug("AssetId:" + edcRequest.assetId)
    logging.debug("thread started")
    return {}


@app.post("/callback/{transferProcessId}/provision/")
async def dummyCallback(transferProcessId: str, request: Request):
    logging.debug(await request.body())
    logging.debug(transferProcessId)
    return {}
