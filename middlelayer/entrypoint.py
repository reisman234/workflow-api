import os
import requests
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

app = FastAPI()

RESOURCE_BASE = os.getenv("RESOURCE_BASE", "/opt/resources")


@app.get("/demo/{case}")
async def getDoDemo(case):

    #reqUrl = "http://141.79.240.3:30910/job/b8ad3c1a-da82-49f5-8ff7-db4f4602a233/resultnew/?result_file=_2022-12-01-11-33-22.bag"
    reqUrl = "http://141.79.240.3:30910/demo"

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
