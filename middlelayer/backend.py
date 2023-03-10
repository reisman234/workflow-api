from typing import List, Dict, Union
from threading import Thread, Event
from time import sleep
from uuid import uuid4
from middlelayer.models import ServiceResouce, ServiceResourceType, WorkflowResource, BaseModel
from middlelayer.k8sClient import k8s_create_config_map, k8s_delete_config_map,\
    k8s_create_pod_manifest, k8s_create_pod, k8s_get_job_info, k8s_delete_pod, \
    k8s_setup_config


class WorkflowBackend():

    def __init__(self):
        pass

    def handle_input(self, workflow_id: str, input_resource: ServiceResouce, data):
        pass

    def commit_workflow(self, workflow_id: str, workflow_resource: WorkflowResource):
        pass

    def stop_workflow(self, workflow_id: str):
        pass

    def get_status(self, workflow_id: str):
        pass

    def cleanup(self):
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
            print("Error: key not known")
            return
        self.data.get(key).job_id = value

    def insert_job_monitor_event(self, key, event: Event):
        if key not in self.data.keys():
            print("Error: key not known")
            return
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

    def handle_input(self, workflow_id: str, input_resource: ServiceResouce, data):
        """
        In case for environment data create a config_map.
        For data create a list which will be downloaded by the init container
        """

        if input_resource.type is ServiceResourceType.environment:
            config_map = k8s_create_config_map(
                data=data,
                namespace=self.namespace)

            self.dummy_db.append_config_map(workflow_id, config_map)
        elif input_resource.type is ServiceResourceType.data:
            raise NotImplementedError()

    def commit_workflow(self, workflow_id, workflow_resource: WorkflowResource):
        job_id = str(uuid4())

        pod_manifest = k8s_create_pod_manifest(
            job_uuid=job_id,
            job_config=workflow_resource,
            job_namespace=self.namespace)

        k8s_create_pod(
            manifest=pod_manifest,
            namespace=self.namespace)

        # TODO if workflow_resource.type is BATCH
        self._create_monitor_thread(
            workflow_id=workflow_id,
            job_id=job_id)

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

    def _cleanup_monitor(self, workflow_id: str):
        stop_event = self.dummy_db.get_job_monitor_event(workflow_id)
        stop_event.set()

    def _create_monitor_thread(self, workflow_id: str, job_id: str):
        stop_event = Event()
        monitor = Thread(target=self._monitor_workflow,
                         args=(job_id, stop_event))
        monitor.start()

        self.dummy_db.insert_job_monitor_event(workflow_id, stop_event)

    def _monitor_workflow(self, job_id: str, stop_event: Event):
        job_completed = False

        while not (job_completed or stop_event.is_set()):
            sleep(10)
            job_info = k8s_get_job_info(job_id)

            if job_info["pod.status.phase"] == "Pending":
                print("JOB STATE: PENDING...")
                sleep(10)
                if job_info["container_states"]:
                    if job_info["container_states"]["dummy-job"]['waiting']['reason'] == "ErrImagePull":
                        raise Exception("FAIL")

            elif job_info["container_states"]["dummy-job"]['terminated'] is None:
                print("JOB RUNNING...")
                sleep(10)
            else:
                print("JOB COMPLETED")
                job_completed = True
