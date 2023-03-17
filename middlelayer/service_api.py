from configparser import ConfigParser
from datetime import datetime, timedelta
from threading import Thread
from fastapi import FastAPI, Depends, HTTPException, Security, Body
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.security.api_key import APIKeyHeader
from starlette.status import HTTP_200_OK, HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND, HTTP_400_BAD_REQUEST

from uuid import uuid4

from middlelayer.imla_minio import ImlaMinio
from middlelayer.models import ServiceDescription, WorkflowResource, WorkflowStoreInfo
from middlelayer.backend import K8sWorkflowBackend, WorkflowBackend


##########
# SECURITY
##########

API_KEY = "pass"

api_key_header = APIKeyHeader(name="access_token", auto_error=False)


async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key == API_KEY:
        return api_key
    else:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN, detail="Could not validate API KEY"
        )

service_api = FastAPI(dependencies=[Depends(get_api_key)])

##########
# DATABASE
##########

SERVICE_ID_CARLA = "carla"
SERVICE_ID = SERVICE_ID_CARLA

USER = "dummy-user"


SERVICE_DESCRIPTION_CARLA = {
    "service_id": SERVICE_ID_CARLA,
    "inputs":  [{"resource_name": "env", "type": 1, "description": "List of environment Variables for Carla Container"}],
    "outputs": [{"resource_name": "rosbag", "type": 2, "description": "Generated rosbag file from .env file"}],
    "workflow_resource": WorkflowResource(worker_image="harbor.gx4ki.imla.hs-offenburg.de/gx4ki/carla:latest",
                                          worker_image_output_directory="/home/carla/rosbag",
                                          gpu=True)
}

SERVICE_ID_DUMMY = "dummy"
SERVICE_DESCRIPTION_DUMMY = {
    "service_id": SERVICE_ID_DUMMY,
    "inputs":  [{"resource_name": "env", "type": 1, "description": "List of environment Variables for dummy-job Container"}],
    "outputs": [{"resource_name": "result", "type": 2, "description": "dummy output file"}],
    "workflow_resource": WorkflowResource(worker_image="harbor.gx4ki.imla.hs-offenburg.de/ralphhso/dummy-job:py3.8-alpine",
                                          worker_image_output_directory="/output/",
                                          gpu=False)
}

service_description_carla = ServiceDescription(**SERVICE_DESCRIPTION_CARLA)

service_description_dummy = ServiceDescription(**SERVICE_DESCRIPTION_DUMMY)

SERVICE_DESCRIPTIONS = {SERVICE_ID_CARLA: service_description_carla,
                        SERVICE_ID_DUMMY: service_description_dummy}

SERVICES = {
    "services": {
        SERVICE_ID_CARLA: {
            "start_date": datetime.now(),
            "end_date:": datetime.now() + timedelta(days=7)
        },
        SERVICE_ID_DUMMY: {
            "start_date": datetime.now(),
            "end_date:": datetime.now() + timedelta(days=7)
        },
    }
}

user_workflow = {}


def add_workflow_id(user_id: str, workflow_id: str):
    if user_id not in user_workflow.keys():
        user_workflow[user_id] = [workflow_id]
    else:
        user_workflow[user_id].append(workflow_id)


def get_workflow_ids(user_id: str):
    if user_id not in user_workflow.keys():
        return []
    else:
        return user_workflow[user_id]

###############
# CONFIGURATION
###############


S_CONFIG = """
[minio]
endpoint = localhost:9000
access_key: root
secret_key: changeme123
secure: False
"""


RESULT_BUCKET = USER+"-storage"
# storage = None
# workflow_backend = None


class ServiceApi():

    def __init__(self):

        config = ConfigParser()
        config.read_string(S_CONFIG)
        self.storage = ImlaMinio(config['minio'], RESULT_BUCKET)

        k8s_namespace = "gx4ki-demo"
        self.workflow_backend: WorkflowBackend = K8sWorkflowBackend(
            namespace=k8s_namespace)

    def get_service_description(self, service_id: str) -> ServiceDescription:
        description = SERVICE_DESCRIPTIONS.get(service_id)
        if description is None:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail="no valid service_id")

        return description

    def generate_resource_upload_url(self, service_id: str, resource_name: str):
        service_description = self.get_service_description(service_id)

        # check resource_name is a valid input
        if resource_name not in [x.resource_name for x in service_description.inputs]:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail="no valid resource provided")

        # generate minio presigned put url
        upload_url = self.storage.get_upload_url(
            RESULT_BUCKET,
            f"{service_id}/inputs/{resource_name}")
        return {"url": upload_url, "method": "put"}

    def generate_resource_download_url(self, service_id, resource_name):
        service_description = self.get_service_description(service_id)
        resource_storage_prefix = f"{service_id}/outputs"
        resource_storage_name = f"{resource_storage_prefix}/{resource_name}"

        # check resource_name is a valid input
        if resource_name not in [x.resource_name for x in service_description.outputs]:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail="no valid resource provided")

        # check if resource exists
        if resource_storage_name not in self.storage.get_objects_list(
                bucket=RESULT_BUCKET,
                prefix=resource_storage_prefix):
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail="requested resource not exists"
            )

        # generate minio presigned put url
        downlaod_url = self.storage.get_download_url(
            RESULT_BUCKET,
            resource=resource_storage_name
        )

        # is a adaption of the url required? e.g. proxy information
        # is a redirection better?

        return {"url": downlaod_url, "method": "get"}

    def service_inputs_exists(self, service_id: str):
        service_description = self.get_service_description(service_id)
        resource_storage_prefix = f"{service_id}/inputs"
        objects_list = self.storage.get_objects_list(
            bucket=RESULT_BUCKET,
            prefix=resource_storage_prefix)
        for resource in service_description.inputs:
            if not f'{resource_storage_prefix}/{resource.resource_name}' in objects_list:
                return False
        return True

    def commit_task(self, service_id, workflow_id):
        service_description = self.get_service_description(service_id)

        for resource in service_description.inputs:
            self.workflow_backend.handle_input(
                workflow_id=workflow_id,
                input_resource=resource,
                get_data_handle=lambda: self.storage.get_resource_data(
                    bucket=RESULT_BUCKET,
                    resource=f"{service_id}/inputs/{resource.resource_name}"))

        self.workflow_backend.commit_workflow(
            workflow_id=workflow_id,
            workflow_resource=service_description.workflow_resource,
            workflow_finished_handle=lambda: self.workflow_finished_handle(
                service_description=service_description,
                workflow_id=workflow_id))

    def workflow_finished_handle(self, service_description: ServiceDescription, workflow_id: str):

        result_files = [i.resource_name for i in service_description.inputs]

        workflow_store_info = WorkflowStoreInfo(
            minio=self.storage.get_store_info(),
            destination_bucket=RESULT_BUCKET,
            destination_path=f"{service_description.service_id}/outputs",
            result_files=result_files)

        self.workflow_backend.store_result(
            workflow_id=workflow_id,
            workflow_store_info=workflow_store_info)

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

        add_workflow_id(
            user_id=USER,
            workflow_id=workflow_id)

    def get_workflow_status(self, service_id: str, workflow_id: str):
        service_description = self.get_service_description(service_id)
        if not self.workflow_exists(service_description, workflow_id):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="invalid workflow_id"
            )
        return self.workflow_backend.get_status(
            workflow_id=workflow_id)

    def workflow_exists(self, service_description: ServiceDescription, workflow_id: str):
        workflow_ids = get_workflow_ids(
            user_id=USER)

        return workflow_id in workflow_ids

    def stop_workflow(self, service_id: str, workflow_id: str):
        service_description = self.get_service_description(service_id)
        if not self.workflow_exists(service_description, workflow_id):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="invalid workflow_id"
            )
        self.workflow_backend.stop_workflow(
            workflow_id=workflow_id)


# client: ServiceApi = None
client: ServiceApi


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
    return SERVICES


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


@service_api.put("/services/{service_id}/input/{resource}")
async def get_service_input_info(service_id: str,
                                 resource: str,
                                 input_file: bytes = Body(..., media_type="text/plain")):
    # pylint: disable=W0613

    """
    Uploads a file into the user storage
    """

    resource_upload = client.generate_resource_upload_url(service_id,
                                                          resource)
    return RedirectResponse(url=resource_upload["url"])


@service_api.get("/services/{service_id}/output/{resource}")
async def get_service_output_info(service_id: str, resource: str):
    """
    return a download url for the specified resource
    """

    resource_download_info = client.generate_resource_download_url(service_id,
                                                                   resource)
    return RedirectResponse(url=resource_download_info["url"])


@service_api.get("/services/{service_id}/workflow/")
async def list_service_workflow(service_id: str):
    """
    returns a list of all running workflows and its IDs.
    """

    return get_workflow_ids(
        user_id=USER)


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
async def get_service_workflow_status(service_id: str, workflow_id: str):
    """
    provides information about a workflow
    get logs of the worker-image of the workflow
    """
    workflow_status = client.get_workflow_status(service_id, workflow_id)

    return JSONResponse(status_code=HTTP_200_OK,
                        content={"service_id": service_id,
                                 "workflow_id": workflow_id,
                                 "workflow_status": workflow_status})
