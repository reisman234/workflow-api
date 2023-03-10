from datetime import datetime, timedelta
from enum import IntEnum, Enum
from pydantic import BaseModel
from typing import List


class Service():
    id: str
    start_date: datetime
    duration: timedelta


class ServiceResourceType(IntEnum):
    environment = 1
    data = 2


class ServiceResouce(BaseModel):
    resource_name: str
    type: ServiceResourceType
    description: str


class WorkflowResource(BaseModel):
    worker_image: str
    worker_image_output_directory: str
    gpu: bool


class ServiceDescription(BaseModel):
    service_id: str
    inputs: List[ServiceResouce]
    outputs: List[ServiceResouce]
    workflow_resource: WorkflowResource


class ContainerSpecs(BaseModel):
    image: str = None
    command: List[str] = None
    args: List[str] = None
