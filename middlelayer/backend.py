from typing import List, Dict, Union, Callable
from threading import Thread, Event
from time import sleep
from uuid import uuid4
from io import StringIO

import sys
import logging

import dotenv

from middlelayer.models import ServiceResouce, ServiceResourceType, WorkflowResource, BaseModel, WorkflowStoreInfo
from middlelayer.k8sClient import K8sPodStateData
from middlelayer.k8sClient import k8s_create_config_map, k8s_delete_config_map,\
    k8s_create_pod_manifest, k8s_create_pod, k8s_get_job_info, k8s_delete_pod, \
    k8s_setup_config,\
    k8s_watch_pod_events, k8s_get_pod_log, \
    k8s_portforward

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


class WorkflowBackend():

    def __init__(self):
        pass

    def handle_input(self,
                     workflow_id: str,
                     input_resource: ServiceResouce,
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
                   verbose_level: int) -> Union[dict, str]:
        pass

    def cleanup(self,
                workflow_id: str):
        pass


class K8sJobData(BaseModel):
    config_maps: Union[List[str], None] = []
    job_id: Union[str, None] = None
    job_monitor_event: Union[Event, None] = None
    job_state: Union[K8sPodStateData, None] = None

    class Config:
        arbitrary_types_allowed = True


class K8sData(BaseModel):
    data: Union[Dict[str, K8sJobData], None]


class SimpleDB():
    def __init__(self):
        self.data: Dict[str, K8sJobData] = dict()

    def append_config_map(self, key: str, data):
        if key not in self.data:
            self.data[key] = K8sJobData()
        self.data[key].config_maps.append(data)

    def get_config_maps(self, key):
        if key not in self.data:
            return []
        return self.data[key].config_maps

    def get_job_data(self, key: str) -> K8sJobData:
        if key not in self.data:
            return None
        return self.data.get(key)

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

    def insert_workflow_state(self, workflow_id: str, pod_data: K8sPodStateData):
        if workflow_id not in self.data:
            self.data[workflow_id] = K8sJobData()
        self.data.get(workflow_id).job_state = pod_data

    def get_workflow_state(self, workflow_id: str) -> K8sPodStateData:
        if workflow_id not in self.data:
            return None
        return self.data.get(workflow_id).job_state

    def delete_entry(self, key):
        if key not in self.data:
            return
        self.data.pop(key)


class K8sWorkflowBackend(WorkflowBackend):

    def __init__(self, namespace):
        self.dummy_db = SimpleDB()
        self.namespace = namespace
        self.labels = {"gx4ki-app": "gx4ki-demo",
                       "gx4ki-job-uuid": "job_uuid"}
        k8s_setup_config()

    def handle_input(self, workflow_id: str, input_resource: ServiceResouce, get_data_handle: Callable):
        """
        In case for environment data create a config_map.
        For data create a list which will be downloaded by the init container
        """
        config_map_id = str(uuid4())

        if input_resource.type is ServiceResourceType.environment:
            config_map_data = dict(dotenv.dotenv_values(
                stream=StringIO(get_data_handle())))

            k8s_create_config_map(
                name=config_map_id,
                namespace=self.namespace,
                data=config_map_data,
                labels={"gx4ki-app": "gx4ki-demo"})

            self.dummy_db.append_config_map(workflow_id, config_map_id)
        elif input_resource.type is ServiceResourceType.data:
            raise NotImplementedError()

    def commit_workflow(self, workflow_id,
                        workflow_resource: WorkflowResource,
                        workflow_finished_handle: Callable):
        job_id = str(uuid4())

        config_map_ids = self.dummy_db.get_config_maps(
            key=workflow_id)

        pod_manifest = k8s_create_pod_manifest(
            job_uuid=job_id,
            job_config=workflow_resource,
            config_map_ref=config_map_ids,
            job_namespace=self.namespace)

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

        if job_data.job_id:
            k8s_delete_pod(
                name=job_data.job_id,
                namespace=self.namespace)

        if job_data.job_monitor_event:
            self._cleanup_monitor(
                workflow_id=workflow_id)

        self.dummy_db.delete_entry(workflow_id)

    def stop_workflow(self,
                      workflow_id: str):

        stop_event = self.dummy_db.get_job_monitor_event(key=workflow_id)
        stop_event.wait()
        self.cleanup(workflow_id=workflow_id)

    def get_status(self,
                   workflow_id: str,
                   verbose_level: int) -> Union[dict, str]:
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

        return {"job_state": job_data.job_state.pod_phase,
                "detail": job_data.job_state.container_statuses["worker"].details}

    def store_result(self,
                     workflow_id,
                     workflow_store_info: WorkflowStoreInfo) -> None:
        job_data = self.dummy_db.get_job_data(workflow_id)
        if not job_data:
            raise KeyError(f"invalid workflow_id: {workflow_id}")

        data_side_car_service = f"http://{job_data.job_id}/store/"
        data_side_car_service = "http://192.168.49.2:32000/store/"

        # TODO
        # - add auth header
        # - use callback
        workflow_backend_logger.debug("""store_workflow_result:
                        \tdata_side_car_service: %s
                        \tworkflow_id: %s
                        \tworkflow_store_info: %s""",
                                      data_side_car_service, workflow_id, workflow_store_info.json())

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
            self.dummy_db.insert_workflow_state(
                workflow_id=workflow_id,
                pod_data=pod_state)

            if stop_event.is_set():
                return True
            if pod_state.container_statuses is None:
                return False
            if pod_state.container_statuses["worker"].state == "terminated":
                return True
            return False

        job_id = self.dummy_db.get_job_data(key=workflow_id).job_id
        # loops until pod_state_handle returns True
        k8s_watch_pod_events(pod_name=job_id,
                             pod_state_handle=pod_state_handle,
                             namespace=self.namespace)
        if stop_event.is_set():
            return

        workflow_finished_handle()

    def monitor_workflow(self,
                         workflow_id: str,
                         stop_event: Event,
                         workflow_finished_handle: Callable[[], None]):
        job_completed = False
        job_id = self.dummy_db.get_job_data(
            key=workflow_id).job_id
        while not (job_completed or stop_event.is_set()):
            sleep(10)
            job_info = k8s_get_job_info(job_id,
                                        namespace=self.namespace)

            if job_info["pod.status.phase"] == "Pending":
                print("JOB STATE: PENDING...")
                sleep(10)
                if job_info["container_states"]:
                    if job_info["container_states"]["worker"]['waiting']['reason'] == "ErrImagePull":
                        raise Exception("FAIL")

            elif job_info["container_states"]["worker"]['terminated'] is None:
                print("JOB RUNNING...")
                sleep(10)
            else:
                print("JOB COMPLETED")
                job_completed = True
                stop_event.set()
                workflow_finished_handle()
