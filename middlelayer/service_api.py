import logging
import os
import sys
import time
from typing import Dict, List, Union
from configparser import ConfigParser
from threading import Thread
from uuid import uuid4

from fastapi import FastAPI, Depends, HTTPException, Security, UploadFile, File
from fastapi.responses import JSONResponse, StreamingResponse, PlainTextResponse, Response
from fastapi.security.api_key import APIKeyHeader
from starlette.status import HTTP_200_OK, HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND, HTTP_400_BAD_REQUEST

from middlelayer.asset import StaticAssetLoader
from middlelayer.imla_minio import ImlaMinio
from middlelayer.models import ServiceDescription, WorkflowStoreInfo
from middlelayer.backend import K8sWorkflowBackend, WorkflowBackend


#########
# LOGGING
#########

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout_handle = logging.StreamHandler(sys.stdout)
stdout_handle.setFormatter(formatter)
stderr_hanlde = logging.StreamHandler(sys.stderr)
stderr_hanlde.setFormatter(formatter)


workflow_api_logger = logging.getLogger("workflow_api")
workflow_api_logger.setLevel(level=logging.DEBUG)
workflow_api_logger.addHandler(stdout_handle)
# workflow_backend_logger.addHandler(stderr_hanlde)

###############
# CONFIGURATION
###############

CONFIG_FILE_PATH = os.environ.get("CONFIG_FILE_PATH", "./config/workflow-api.cfg")

workflow_api_logger.debug("load config file %s", {CONFIG_FILE_PATH})

CONFIG = ConfigParser()
CONFIG.read(CONFIG_FILE_PATH)

if not CONFIG.has_section("workflow_api"):
    raise ValueError("config has no workflow_api section")
WORKFLOW_API_CONFIG = CONFIG["workflow_api"]

WORKFLOW_API_USER = WORKFLOW_API_CONFIG.get("workflow_api_user")
WORKFLOW_API_ACCESS_TOKEN = WORKFLOW_API_CONFIG.get("workflow_api_access_token")
WORKFLOW_API_USER_STORAGE = WORKFLOW_API_USER+"-storage"
WORKFLOW_API_INSTANT_REMOVAL = WORKFLOW_API_CONFIG.getboolean("workflow_api_instant_removal", True)

if not CONFIG.has_section("minio"):
    raise ValueError("config has no minio section")
MINIO_CONFIG = CONFIG["minio"]


##########
# SECURITY
##########


api_key_header = APIKeyHeader(name="access-token", auto_error=False)


async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key == WORKFLOW_API_ACCESS_TOKEN:
        return api_key
    else:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN, detail="Could not validate API KEY"
        )

workflow_api_logger.debug("set root_path=%s", os.getenv("FASTAPI_ROOT_PATH"))
service_api = FastAPI(root_path=os.getenv("FASTAPI_ROOT_PATH"), dependencies=[Depends(get_api_key)])

##########
# DATABASE
##########

asset_store = StaticAssetLoader()


user_workflow: Dict[str, List[str]] = {}


def remove_workflow_id(user_id: str, workflow_id: str):
    if user_id not in user_workflow.keys():
        user_workflow[user_id] = [workflow_id]
    else:
        user_workflow[user_id].remove(workflow_id)


def insert_workflow_id(user_id: str, workflow_id: str):
    if user_id not in user_workflow.keys():
        user_workflow[user_id] = [workflow_id]
    else:
        user_workflow[user_id].append(workflow_id)


def get_workflow_ids(user_id: str):
    if user_id not in user_workflow.keys():
        return []
    else:
        return user_workflow[user_id]


class ServiceApi():

    def __init__(self):

        self.storage = ImlaMinio(MINIO_CONFIG, WORKFLOW_API_USER_STORAGE)

        if WORKFLOW_API_CONFIG.get("workflow_backend") == "kubernetes":
            self.workflow_backend: WorkflowBackend = K8sWorkflowBackend(
                kubeconfig=WORKFLOW_API_CONFIG.get("workflow_backend_kubeconfig"),
                namespace=WORKFLOW_API_CONFIG.get("workflow_backend_namespace"),
                image_pull_secret=WORKFLOW_API_CONFIG.get("workflow_backend_image_pull_secret"),
                data_side_car_image=WORKFLOW_API_CONFIG.get("workflow_backend_data_side_car_image"))

    def get_service_description(self, service_id: str) -> ServiceDescription:
        description = asset_store.get_assets_description(service_id)
        if description is None:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail="no valid service_id")

        return description

    def put_resource(self, service_id: str, resource_name: str, resource_file: UploadFile):
        service_description = self.get_service_description(service_id)

        # check resource_name is a valid input
        if resource_name not in [x.resource_name for x in service_description.inputs]:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail="no valid resource provided")

        self.storage.put_file(
            bucket=WORKFLOW_API_USER_STORAGE,
            resource=f"{service_id}/inputs/{resource_name}",
            file=resource_file)

    def get_resource(self, service_id, resource_name):
        service_description = self.get_service_description(service_id)
        resource_storage_prefix = f"{service_id}/outputs/"
        resource_storage_name = f"{resource_storage_prefix}{resource_name}"

        # check resource_name is a valid input
        if resource_name not in [x.resource_name for x in service_description.outputs]:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail="no valid resource provided")

        # check if resource exists
        if resource_name not in self.storage.get_objects_list(
                bucket=WORKFLOW_API_USER_STORAGE,
                prefix=resource_storage_prefix):
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail="requested resource not exists"
            )

        return self.storage.get_file(
            bucket=WORKFLOW_API_USER_STORAGE,
            resource=resource_storage_name
        )

    def get_workflow_result(self, service_id, workflow_id, result_file):
        resource_storage_prefix = f"{service_id}/outputs/{workflow_id}"
        resource_storage_name = f"{resource_storage_prefix}/{result_file}"

        return self.storage.get_file(
            bucket=WORKFLOW_API_USER_STORAGE,
            resource=resource_storage_name
        )

    def service_inputs_exists(self, service_id: str):
        service_description = self.get_service_description(service_id)
        resource_storage_prefix = f"{service_id}/inputs"
        objects_list = self.storage.get_objects_list(
            bucket=WORKFLOW_API_USER_STORAGE,
            prefix=resource_storage_prefix)
        for resource in service_description.inputs:
            if f'{resource_storage_prefix}/{resource.resource_name}' not in objects_list:
                return False
        return True

    def commit_task(self, service_id, workflow_id):
        service_description = self.get_service_description(service_id)

        for resource in service_description.inputs:
            self.workflow_backend.handle_input(
                workflow_id=workflow_id,
                input_resource=resource,
                get_data_handle=lambda: self.storage.get_resource_data(
                    bucket=WORKFLOW_API_USER_STORAGE,
                    resource=f"{service_id}/inputs/{resource.resource_name}"))

        self.workflow_backend.commit_workflow(
            workflow_id=workflow_id,
            workflow_resource=service_description.workflow_resource,
            workflow_finished_handle=lambda: self.workflow_finished_handle(
                service_description=service_description,
                workflow_id=workflow_id))

    def workflow_finished_handle(self, service_description: ServiceDescription, workflow_id: str):

        result_files = [i.resource_name for i in service_description.outputs]

        workflow_store_info = WorkflowStoreInfo(
            minio=self.storage.get_store_info(),
            destination_bucket=WORKFLOW_API_USER_STORAGE,
            destination_path=f"{service_description.service_id}/outputs",
            result_files=result_files)

        self.workflow_backend.store_result(
            workflow_id=workflow_id,
            workflow_store_info=workflow_store_info)

        if not WORKFLOW_API_INSTANT_REMOVAL:
            workflow_api_logger.debug("WORKFLOW_API_INSTANT_REMOVAL disabled")
            time.sleep(900)

        self.workflow_backend.cleanup(
            workflow_id=workflow_id)

    def commit_workflow(self, service_id, workflow_id):
        """
        threaded task to to deploy workload into backend
        """
        commit_thread = Thread(target=self.commit_task,
                               name="commit_task",
                               args=[service_id, workflow_id])
        commit_thread.start()

        insert_workflow_id(user_id=WORKFLOW_API_USER,
                           workflow_id=workflow_id)

    def get_workflow_status(self,
                            service_id: str,
                            workflow_id: str,
                            verbose_level: int):
        service_description = self.get_service_description(service_id)
        if not self.workflow_exists(service_description, workflow_id):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="invalid workflow_id"
            )
        return self.workflow_backend.get_status(
            workflow_id=workflow_id,
            verbose_level=verbose_level)

    def workflow_exists(self, service_description: ServiceDescription, workflow_id: str):
        workflow_ids = get_workflow_ids(
            user_id=WORKFLOW_API_USER)

        return workflow_id in workflow_ids

    def stop_workflow(self, service_id: str, workflow_id: str):
        service_description = self.get_service_description(service_id)
        if not self.workflow_exists(service_description, workflow_id):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="invalid workflow_id"
            )
        self.workflow_backend.cleanup(
            workflow_id=workflow_id)

    def list_workflow_results(self, service_id: str, workflow_id: str):
        service_description = self.get_service_description(service_id)
        # TODO check if workflow_id is in history or it exists as a path prefix

        workflow_result_prefix = f"{service_id}/outputs/{workflow_id}/"
        object_list = self.storage.get_objects_list(
            bucket=WORKFLOW_API_USER_STORAGE,
            prefix=workflow_result_prefix)

        return object_list


client: ServiceApi = None


@service_api.on_event("startup")
async def startup():

    global client
    client = ServiceApi()

    print("STARTUP")


@service_api.get("/services/")
async def get_services():
    """
    list available services
    """
    return asset_store.get_assets()


@service_api.get("/services/{service_id}/info")
async def get_service_info(service_id: str):
    """
    returns informations to a requested service
    """
    service_description = client.get_service_description(service_id)

    return service_description

# curl -H "content-type: text/plain "  -H 'access_token: password' -XPUT $UPLOAD_URL   --data-binary @resources/test.env


# @service_api.get("/services/{service_id}/input/{resource}")
# async def get_service_input_info(service_id: str, resource: str):
#     """
#     return a upload url for the specified resource
#     """

#     resource_upload = client.generate_resource_upload_url(service_id,
#                                                           resource)

#     return resource_upload


# @service_api.put("/services/{service_id}/input/{resource}")
# async def get_service_input_info(service_id: str,
#                                  resource: str,
#                                  input_file: bytes = Body(..., media_type="text/plain")):
#     # pylint: disable=W0613

#     """
#     Uploads a file into the user storage
#     """

#     resource_upload = client.generate_resource_upload_url(service_id,
#                                                           resource)
#     return RedirectResponse(url=resource_upload["url"])


# @service_api.get("/services/{service_id}/output/{resource}")
# async def get_service_output_info(service_id: str, resource: str):
#     """
#     return a download url for the specified resource
#     """

#     resource_download_info = client.generate_resource_download_url(service_id,
#                                                                    resource)
#     return RedirectResponse(url=resource_download_info["url"])

@service_api.put("/services/{service_id}/input/{resource}")
async def put_service_input_info(service_id: str,
                                 resource: str,
                                 input_file: UploadFile = File(...)):
    # pylint: disable=W0613

    """
    upload service specific file into the user storage
    """
    resource_upload = client.put_resource(
        service_id=service_id,
        resource_name=resource,
        resource_file=input_file)

    return {}


@service_api.get("/services/{service_id}/output/{resource}")
async def get_service_output_info(service_id: str, resource: str):
    """
    download a generated result file from the user storage
    """
    KB = 1024
    response = None
    try:
        response = client.get_resource(service_id,
                                       resource)

        # Set the Content-Disposition header so the browser
        # knows to download the file instead of displaying it
        headers = {}
        headers["content-type"] = response.headers.get("Content-Type")
        headers["content-length"] = response.headers.get("Content-Length")
        # headers["transfer-encoding"] = "chunked"
        headers["content-disposition"] = f"attachment"

        return Response(
            content=response.read(),
            headers=headers)
    finally:
        if response:
            response.close()
            response.release_conn()


@service_api.get("/services/{service_id}/workflow/results/{workflow_id}",)
async def get_workflow_results(service_id: str, workflow_id: str, result_file: Union[str, None] = None):
    """
    if result_file=None, list the available result files for a executed workflow.
    else the provided result_file will be downloaded.
    """

    object_list = client.list_workflow_results(service_id=service_id,
                                               workflow_id=workflow_id)

    if result_file:

        if result_file not in object_list:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=f"unknown result_file {result_file}"
            )

        response = client.get_workflow_result(service_id=service_id,
                                              workflow_id=workflow_id,
                                              result_file=result_file)

        def iter_content():
            # Streaming the content in chunks to avoid loading everything into memory
            for chunk in response.stream():
                yield chunk

        # Set the appropriate content headers in the response
        # response.headers["Content-Disposition"] = f"attachment; filename={result_file}"
        headers = {}
        headers["Content-Type"] = response.headers.get("Content-Type")
        headers["Content-Length"] = response.headers.get("Content-Length")
        # headers["transfer-encoding"] = "chunked"
        headers["Content-Disposition"] = f"attachment; filename={result_file}"

        # Use StreamingResponse to return the content in chunks
        return StreamingResponse(iter_content(), headers=headers, media_type="application/octet-stream")

    return {"result_objects": object_list}


@service_api.get("/services/{service_id}/workflow/")
async def list_service_workflow(service_id: str):
    """
    returns a list of all running workflows and its IDs.
    """

    return get_workflow_ids(
        user_id=WORKFLOW_API_USER)


@service_api.post("/services/{service_id}/workflow/execute")
async def start_service_workflow(service_id: str):
    """
    triggers the creation and execution the workflow for this service.
    """

    workflow_id = str(uuid4())

    # TODO check user workflow limit

    # checks if provided inputs are met with service description
    if not client.service_inputs_exists(service_id):
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail="service input not fulfilled")

    client.commit_workflow(service_id, workflow_id)

    return {"workflow_id": workflow_id}


@service_api.post("/services/{service_id}/workflow/stop/{workflow_id}")
async def stops_service_workflow(service_id: str, workflow_id: str):
    """
    stops and deletes workflow resources.
    """

    # check if job was executed and not already stopped
    # trigger cleanup
    #   - remove job and config maps
    client.stop_workflow(service_id, workflow_id)

    return {}


@service_api.get("/services/{service_id}/workflow/status/{workflow_id}")
async def get_service_workflow_status(service_id: str, workflow_id: str, verbose_level: int = 0):
    """
    provides information about a workflow
    get logs of the worker-image of the workflow
    """
    if verbose_level not in [0, 1, 2]:
        verbose_level = 0

    workflow_status = client.get_workflow_status(service_id=service_id,
                                                 workflow_id=workflow_id,
                                                 verbose_level=verbose_level)
    if verbose_level == 0:
        return JSONResponse(status_code=HTTP_200_OK,
                            content={"service_id": service_id,
                                     "workflow_id": workflow_id,
                                     "workflow_status": workflow_status})
    if verbose_level in [1, 2]:
        return PlainTextResponse(status_code=HTTP_200_OK,
                                 content=workflow_status)
