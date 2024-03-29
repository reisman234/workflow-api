from typing import List, Dict, Union, Callable
from threading import Thread, Event
from uuid import uuid4
from io import StringIO
from enum import Enum

import sys
import logging
import json

import dotenv


from middlelayer.models import (
    ServiceResourceType, WorkflowResource, BaseModel, WorkflowStoreInfo, WorkflowInputResource,
    K8sBackendConfig, K8sStorageType)

from middlelayer.k8sClient import K8sPodStateData
from middlelayer.k8sClient import k8s_create_config_map, k8s_delete_config_map,\
    k8s_create_pod_manifest, k8s_create_pod, k8s_delete_pod, \
    k8s_setup_config,\
    k8s_watch_pod_events, k8s_get_pod_log, \
    k8s_portforward, \
    k8s_create_persistent_volume_claim, k8s_delete_persistent_volume_claim

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout_handle = logging.StreamHandler(sys.stdout)
stdout_handle.setFormatter(formatter)
stderr_hanlde = logging.StreamHandler(sys.stderr)
stderr_hanlde.setFormatter(formatter)


workflow_backend_logger = logging.getLogger("workflow_backend")
workflow_backend_logger.setLevel(level=logging.DEBUG)
workflow_backend_logger.addHandler(stdout_handle)
# workflow_backend_logger.addHandler(stderr_hanlde)


class WorkflowJobPhase(str, Enum):
    PREPARING = "PREPARING"
    RUNNING = "RUNNING"
    STORING = "STORING"
    FINISHED = "FINISHED"
    CANCELED = "CANCELED"


class WorkflowJobState(BaseModel):
    phase: WorkflowJobPhase = WorkflowJobPhase.PREPARING
    worker_state: Union[K8sPodStateData, None] = None

    class Config:
        json_encoders = {WorkflowJobPhase: lambda p: p.name}


class WorkflowInputConfig(BaseModel):
    id: str = None
    inputs: List[WorkflowInputResource] = []

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.id = str(uuid4())


class K8sJobData(BaseModel):
    config_maps: Union[List[str], None] = []
    input_config: WorkflowInputConfig = None
    volume_claim_id: Union[str, None] = None
    job_id: Union[str, None] = None
    job_monitor_event: Union[Event, None] = None
    job_state: Union[WorkflowJobState, None] = WorkflowJobState()

    class Config:
        arbitrary_types_allowed = True


class K8sData(BaseModel):
    data: Union[Dict[str, K8sJobData], None]


class WorkflowBackend():

    def __init__(self):
        pass

    def handle_input(self,
                     workflow_id: str,
                     input_resource: WorkflowInputResource,
                     get_data_handle: Callable):
        pass

    def commit_workflow(self,
                        workflow_id: str,
                        workflow_resource: WorkflowResource,
                        workflow_finished_handle: Callable):
        pass

    def stop_workflow(self,
                      workflow_id: str):
        pass

    def store_result(self,
                     workflow_id: str,
                     workflow_store_info: WorkflowStoreInfo) -> None:
        pass

    def get_status(self,
                   workflow_id: str,
                   verbose_level: int) -> Union[WorkflowJobState, str]:
        pass

    def cleanup(self,
                workflow_id: str):
        pass


class SimpleDB():
    def __init__(self):
        self.data: Dict[str, K8sJobData] = dict()

    def append_config_map(self, key: str, data):
        if key not in self.data:
            self.data[key] = K8sJobData()
        self.data[key].config_maps.append(data)

    def insert_input_resource(self, key: str, input_resource: WorkflowInputResource) -> None:
        if key not in self.data:
            self.data[key] = K8sJobData()
        if not self.data[key].input_config:
            self.data[key].input_config = WorkflowInputConfig()
        self.data[key].input_config.inputs.append(input_resource)

    def get_config_maps(self, key):
        if key not in self.data:
            return []
        return self.data[key].config_maps

    def get_input_config(self, key: str) -> WorkflowInputConfig:
        if key not in self.data:
            return None
        return self.data[key].input_config

    def get_job_data(self, key: str) -> K8sJobData:
        if key not in self.data:
            return None
        return self.data.get(key)

    def set_job_volume_claim_id(self, key: str, volume_claim_id: str):
        if key not in self.data:
            self.data[key] = K8sJobData()
        self.data.get(key).volume_claim_id = volume_claim_id

    def insert_job_id(self, key: str, value: str):
        if key not in self.data:
            self.data[key] = K8sJobData()
        self.data.get(key).job_id = value

    def insert_job_monitor_event(self, key, event: Event):
        if key not in self.data:
            self.data[key] = K8sJobData()
        self.data.get(key).job_monitor_event = event

    def get_job_monitor_event(self, key) -> Event:
        if key not in self.data:
            return None
        return self.data.get(key).job_monitor_event

    def insert_workflow_state(self, workflow_id: str, job_state: WorkflowJobState):
        if workflow_id not in self.data:
            self.data[workflow_id] = K8sJobData()
        self.data.get(workflow_id).job_state = job_state

    def get_workflow_state(self, workflow_id: str) -> WorkflowJobState:
        if workflow_id not in self.data:
            return None
        return self.data.get(workflow_id).job_state

    def set_workflow_job_finished(self, workflow_id: str):
        if workflow_id not in self.data:
            return None
        self.data.get(workflow_id).job_state.phase = WorkflowJobPhase.FINISHED

    def delete_entry(self, key):
        if key not in self.data:
            return
        self.data.pop(key)


class K8sWorkflowBackend(WorkflowBackend):

    def __init__(self,
                 namespace,
                 kubeconfig=None,
                 image_pull_secret=None,
                 data_side_car_image=None,
                 k8s_backend_config: K8sBackendConfig = None):
        self.dummy_db = SimpleDB()
        self.namespace = namespace

        if k8s_backend_config:
            self.k8s_backend_config = K8sBackendConfig.model_validate(k8s_backend_config.model_dump(exclude_unset=True,
                                                                                                    exclude_none=True))

        else:
            self.k8s_backend_config = K8sBackendConfig()

        k8s_setup_config(
            k8s_backend_config=self.k8s_backend_config,
            config_file=kubeconfig,
            image_pull_secret=image_pull_secret,
            data_side_car_image=data_side_car_image)

    def handle_input(self,
                     workflow_id: str,
                     input_resource: WorkflowInputResource,
                     get_data_handle: Callable):
        """
        In case for environment data create a config_map.
        For data create a list which will be downloaded by the init container
        """

        if input_resource.type is ServiceResourceType.environment:
            config_map_id = str(uuid4())
            config_map_data = dict(dotenv.dotenv_values(
                stream=StringIO(get_data_handle())))

            k8s_create_config_map(
                name=config_map_id,
                namespace=self.namespace,
                data=config_map_data,
                labels=self.__get_lable(workflow_id=workflow_id))

            self.dummy_db.append_config_map(workflow_id, config_map_id)
        else:
            self.dummy_db.insert_input_resource(workflow_id, input_resource)

    def commit_workflow(self, workflow_id,
                        workflow_resource: WorkflowResource,
                        workflow_finished_handle: Callable):
        job_id = str(uuid4())

        workflow_lables = self.__get_lable(
            workflow_id=workflow_id,
            job_id=job_id
        )

        input_config_id, input_resources = self.__create_input_config_ref(
            workflow_id=workflow_id,
            labels=workflow_lables)

        config_map_ids = self.dummy_db.get_config_maps(key=workflow_id)

        persistent_volume_claim_id = None
        if self.k8s_backend_config.job_storage_type != K8sStorageType.EMPTY_DIR:

            if self.k8s_backend_config.job_storage_type is K8sStorageType.PERSISTENT_VOLUME_CLAIM:
                persistent_volume_claim_id = str(uuid4())
                k8s_create_persistent_volume_claim(
                    name=persistent_volume_claim_id,
                    namespace=self.namespace,
                    storage_size_in_Gi=self.k8s_backend_config.job_storage_size,
                    labels=workflow_lables
                )
                self.dummy_db.set_job_volume_claim_id(workflow_id,
                                                      persistent_volume_claim_id)
            else:
                raise ValueError("unknown k8s storage type")

        pod_manifest = k8s_create_pod_manifest(
            job_uuid=job_id,
            job_config=workflow_resource,
            config_map_ref=config_map_ids,
            input_config_ref=input_config_id,
            input_resources=input_resources,
            job_namespace=self.namespace,
            persistent_volume_claim_id=persistent_volume_claim_id,
            labels=workflow_lables
        )

        k8s_create_pod(
            manifest=pod_manifest,
            namespace=self.namespace)

        self.dummy_db.insert_job_id(
            workflow_id,
            job_id)

        # TODO if workflow_resource.type is BATCH
        # difference between interactive and long running job
        self.__create_monitor_thread(
            workflow_id=workflow_id,
            workflow_finished_handle=workflow_finished_handle
        )

    def cleanup(self, workflow_id):
        """
        removes all k8s resources (pod and configmaps) for a specific workflow
        """
        job_data = self.dummy_db.get_job_data(workflow_id)

        if job_data is None:
            return

        for config_map_id in job_data.config_maps:
            k8s_delete_config_map(config_map_id,
                                  self.namespace)

        if job_data.input_config:
            k8s_delete_config_map(
                name=job_data.input_config.id,
                namespace=self.namespace
            )

        if job_data.volume_claim_id:
            k8s_delete_persistent_volume_claim(name=job_data.volume_claim_id,
                                               namespace=self.namespace)

        if job_data.job_id:
            k8s_delete_pod(
                name=job_data.job_id,
                namespace=self.namespace)

        if job_data.job_monitor_event:
            self._cleanup_monitor(
                workflow_id=workflow_id)

        self.dummy_db.set_workflow_job_finished(workflow_id)

    def stop_workflow(self,
                      workflow_id: str):

        stop_event = self.dummy_db.get_job_monitor_event(key=workflow_id)
        stop_event.wait()
        self.cleanup(workflow_id=workflow_id)

    def get_status(self,
                   workflow_id: str,
                   verbose_level: int) -> Union[WorkflowJobState, str]:
        job_data = self.dummy_db.get_job_data(workflow_id)

        if verbose_level == 1:
            return k8s_get_pod_log(
                pod_name=job_data.job_id,
                container="worker",
                namespace=self.namespace)
        if verbose_level == 2:
            return k8s_get_pod_log(
                pod_name=job_data.job_id,
                container="worker",
                namespace=self.namespace,
                tail_lines=None)

        return job_data.job_state

    def store_result(self,
                     workflow_id,
                     workflow_store_info: WorkflowStoreInfo) -> None:
        job_data = self.dummy_db.get_job_data(workflow_id)
        if not job_data:
            raise KeyError(f"invalid workflow_id: {workflow_id}")

        # TODO
        # - add auth header
        # - use callback
        workflow_backend_logger.debug("""store_workflow_result:
                        \tworkflow_id: %s
                        \tworkflow_store_info: %s""",
                                      workflow_id, workflow_store_info.json())

        status_code = k8s_portforward(data=workflow_store_info.json(),
                                      name=job_data.job_id,
                                      namespace=self.namespace)
        if status_code >= 400:
            workflow_backend_logger.error("request to data-side-car failed %s",
                                          status_code)
            # TODO what todo if request fail?

        workflow_backend_logger.info("store result successful %s",
                                     status_code)

    def _cleanup_monitor(self, workflow_id: str):
        stop_event = self.dummy_db.get_job_monitor_event(workflow_id)
        stop_event.set()

    def __create_input_config_ref(self,
                                  workflow_id: str,
                                  labels: dict = None):
        input_config = self.dummy_db.get_input_config(workflow_id)

        if not input_config:
            return (None, None)

        input_config_json = [item.model_dump() for item in input_config.inputs]

        k8s_create_config_map(
            name=str(input_config.id),
            namespace=self.namespace,
            data={"input-init.json": json.dumps(input_config_json)},
            labels=labels)

        return (input_config.id, input_config.inputs)

    def __create_monitor_thread(self,
                                workflow_id: str,
                                workflow_finished_handle: Callable):
        stop_event = Event()
        monitor = Thread(target=self.__monitor_workflow,
                         args=(workflow_id, stop_event, workflow_finished_handle))
        monitor.start()

        self.dummy_db.insert_job_monitor_event(workflow_id, stop_event)

    def __monitor_workflow(self,
                           workflow_id: str,
                           stop_event: Event,
                           workflow_finished_handle: Callable[[], None]):

        def pod_state_handle(pod_state: K8sPodStateData):
            workflow_backend_logger.debug(pod_state)

            can_exit = False
            phase = WorkflowJobPhase.PREPARING
            if stop_event.is_set():
                can_exit = True
                phase = WorkflowJobPhase.CANCELED
            elif pod_state.container_statuses is None:
                can_exit = False
                phase = WorkflowJobPhase.PREPARING
            elif pod_state.container_statuses["worker"].state == "running":
                can_exit = False
                phase = WorkflowJobPhase.RUNNING
            elif pod_state.container_statuses["worker"].state == "terminated":
                can_exit = True
                phase = WorkflowJobPhase.STORING

            self.dummy_db.insert_workflow_state(
                workflow_id=workflow_id,
                job_state=WorkflowJobState(phase=phase,
                                           worker_state=pod_state)
            )

            return can_exit

        job_id = self.dummy_db.get_job_data(key=workflow_id).job_id
        # loops until pod_state_handle returns True
        k8s_watch_pod_events(pod_name=job_id,
                             pod_state_handle=pod_state_handle,
                             namespace=self.namespace)
        if stop_event.is_set():
            return

        workflow_finished_handle()

    def __get_lable(self, workflow_id=None, job_id=None):
        lable = {"app": "gx4ki-demo"}
        if workflow_id:
            lable["workflow-id"] = workflow_id
        if job_id:
            lable["job-id"] = job_id

        return lable
