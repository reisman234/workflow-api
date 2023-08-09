# pylint: disable=no-name-in-module
from typing import List, Union
from datetime import datetime, timedelta
from enum import IntEnum
from pydantic import BaseModel


class Service():
    id: str
    start_date: datetime
    duration: timedelta


class ServiceResourceType(IntEnum):
    environment = 1
    data = 2
    data_zip = 3

# TODO environment type data size in k8s can only be 1MB in v1.21 and 8MB v1.22,
# it depends on the etcd


class WorkflowInputResource(BaseModel):
    resource_name: str
    type:  ServiceResourceType
    storage_source: str
    mount_path: Union[str, None] = None
    description: str


class ServiceResouce(BaseModel):
    resource_name: str
    type: ServiceResourceType
    description: str


class InputServiceResource(ServiceResouce):
    mount_path: Union[str, None] = None


class WorkflowResource(BaseModel):
    worker_image: str
    worker_image_output_directory: Union[str, None]
    worker_image_args: Union[List[str], None] = None
    worker_image_command: Union[List[str], None] = None
    gpu: bool


class ServiceDescription(BaseModel):
    service_id: str
    inputs: List[InputServiceResource]
    outputs: List[ServiceResouce]
    workflow_resource: WorkflowResource


class ContainerSpecs(BaseModel):
    image: str = None
    command: List[str] = None
    args: List[str] = None


class MinioStoreInfo(BaseModel):
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool


class WorkflowStoreInfo(BaseModel):
    minio: MinioStoreInfo
    destination_bucket: str
    destination_path: str
    result_directory: str = "/output"
    result_files: List[str]
