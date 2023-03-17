from typing import List, Dict, Union, Callable
from threading import Thread, Event
from time import sleep
from uuid import uuid4
import dotenv
import requests
from io import StringIO
from middlelayer.models import ServiceResouce, ServiceResourceType, WorkflowResource, BaseModel, WorkflowStoreInfo
from middlelayer.k8sClient import k8s_create_config_map, k8s_delete_config_map,\
    k8s_create_pod_manifest, k8s_create_pod, k8s_get_job_info, k8s_delete_pod, \
    k8s_setup_config


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
                   workflow_id: str):
        pass

    def cleanup(self,
                workflow_id: str):
        pass


class K8sJobData(BaseModel):
    config_maps: Union[List[str], None] = []
    job_id: Union[str, None] = None
    job_monitor_event: Union[Event, None] = None

    class Config:
        arbitrary_types_allowed = True


class K8sData(BaseModel):
    data: Union[Dict[str, K8sJobData], None]


class SimpleDB():
    def __init__(self):
        self.data: Dict[str, K8sJobData] = dict()

    def append_config_map(self, key: str, data):
        if key not in self.data.keys():
            self.data[key] = K8sJobData()
        self.data[key].config_maps.append(data)

    def get_config_maps(self, key):
        if key not in self.data.keys():
            return []
        return self.data[key].config_maps

    def get_job_data(self, key: str) -> K8sJobData:
        if key not in self.data.keys():
            return None
        return self.data.get(key)

    def insert_job_id(self, key: str, value: str):
        if key not in self.data.keys():
            self.data[key] = K8sJobData()
        self.data.get(key).job_id = value

    def insert_job_monitor_event(self, key, event: Event):
        if key not in self.data.keys():
            self.data[key] = K8sJobData()
        self.data.get(key).job_monitor_event = event

    def get_job_monitor_event(self, key) -> Event:
        if key not in self.data.keys():
            return None
        return self.data.get(key).job_monitor_event

    def delete_entry(self, key):
        if key not in self.data.keys():
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
                data=config_map_data)

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

        

        # TODO if workflow_resource.type is BATCH
        self.__create_monitor_thread(
            workflow_id=workflow_id,
            job_id=job_id,
            workflow_finished_handle=workflow_finished_handle
        )

        self.dummy_db.insert_job_id(
            workflow_id,
            job_id)

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

    def stop_workflow(self, workflow_id: str):
        raise NotImplementedError()

    def get_status(self, workflow_id: str):
        raise NotImplementedError()

    def store_result(self, workflow_id, workflow_store_info: WorkflowStoreInfo) -> None:
        job_data = self.dummy_db.get_job_data(workflow_id)
        if not job_data:
            raise KeyError(f"invalid workflow_id: {workflow_id}")

        data_side_car_service = f"http://{job_data.job_id}/store_result/"

        # TODO
        # - add auth header
        # - what todo if request fail?
        response = requests.post(
            url=data_side_car_service,
            data=workflow_store_info
        )

    def _cleanup_monitor(self, workflow_id: str):
        stop_event = self.dummy_db.get_job_monitor_event(workflow_id)
        stop_event.set()

    def __create_monitor_thread(self, workflow_id: str,
                                job_id: str,
                                workflow_finished_handle: Callable):
        stop_event = Event()
        monitor = Thread(target=self.monitor_workflow,
                         args=(job_id, stop_event, workflow_finished_handle))
        monitor.start()

        self.dummy_db.insert_job_monitor_event(workflow_id, stop_event)

    def monitor_workflow(self, job_id: str,
                         stop_event: Event,
                         workflow_finished_handle: Callable[[], None]):
        job_completed = False

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
