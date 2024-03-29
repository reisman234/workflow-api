from typing import List, Dict, Union

import os
import sys
import logging
import urllib3
import requests

from kubernetes import client, config, watch
from kubernetes.stream import portforward
from kubernetes.client.exceptions import ApiException

from middlelayer.decorators import retry
from middlelayer.models import (WorkflowResource, BaseModel, WorkflowInputResource, ServiceResourceType,
                                K8sBackendConfig)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout_handle = logging.StreamHandler(sys.stdout)
stdout_handle.setFormatter(formatter)
stderr_hanlde = logging.StreamHandler(sys.stderr)
stderr_hanlde.setFormatter(formatter)

k8sclient_logger = logging.getLogger("k8sclient")
k8sclient_logger.setLevel(level=logging.DEBUG)
k8sclient_logger.addHandler(stdout_handle)


class K8sContainerStateDate(BaseModel):
    state: str
    details: str


class K8sPodStateData(BaseModel):
    event_type: str
    pod_phase: str
    pod_state_condition: Union[List[str], None]
    container_statuses: Union[Dict[str, K8sContainerStateDate], None]


NAMESPACE = "default"
IMAGE_PULL_SECRET = "imla-registry"

DATA_SIDE_CAR_IMAGE = "imlahso/data-side-car:latest"

K8S_BACKEND_CONFIG: K8sBackendConfig = None


def k8s_setup_config(k8s_backend_config: K8sBackendConfig,
                     config_file=None,
                     image_pull_secret=None,
                     data_side_car_image=None):

    assert k8s_backend_config != None

    global K8S_BACKEND_CONFIG
    K8S_BACKEND_CONFIG = k8s_backend_config

    if config_file:
        # load a specific config_file
        config.load_kube_config(config_file=config_file)
    else:
        config.load_config()

    if image_pull_secret:
        global IMAGE_PULL_SECRET
        IMAGE_PULL_SECRET = image_pull_secret

    if data_side_car_image:
        global DATA_SIDE_CAR_IMAGE
        DATA_SIDE_CAR_IMAGE = data_side_car_image


def k8s_get_healthz():
    return client.ApiClient().call_api(resource_path="/healthz",
                                       method="GET",
                                       #  query_params={"verbose": "true"},
                                       response_type=str)


def k8s_create_pod_manifest(job_uuid,
                            job_config: WorkflowResource,
                            config_map_ref: List[str] = None,
                            input_config_ref: str = None,
                            input_resources: List[WorkflowInputResource] = None,
                            job_namespace=NAMESPACE,
                            persistent_volume_claim_id: str = None,
                            labels=None) -> client.V1Pod:

    JOB_VOLUME_NAME = "workflow-job-volume"

    data_init_container = None
    containers = []
    worker_container_volume_mounts = []
    pod_spec_volumes = []
    pod_spec_init_containers = []

    workflow_job_volume = None
    if persistent_volume_claim_id:
        workflow_job_volume = client.V1Volume(
            name=JOB_VOLUME_NAME,
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                claim_name=persistent_volume_claim_id)
        )

    # if no workflow_job_volume use EmptyDir as fallback
    if not workflow_job_volume:
        workflow_job_volume = client.V1Volume(
            name=JOB_VOLUME_NAME,
            empty_dir=client.V1EmptyDirVolumeSource(
                size_limit=K8S_BACKEND_CONFIG.job_storage_size)
        )

    pod_spec_volumes.append(
        workflow_job_volume
    )

    if input_config_ref:

        pod_spec_volumes.append(
            client.V1Volume(
                name="workflow-api-config",
                secret=client.V1SecretVolumeSource(
                    secret_name="workflow-api-config"
                )
            )
        )

        pod_spec_volumes.append(
            client.V1Volume(
                name="input-init-config",
                config_map=client.V1ConfigMapVolumeSource(
                    name=input_config_ref,
                    items=[client.V1KeyToPath(key="input-init.json", path="input-init.json")])
            )
        )

        data_init_container = client.V1Container(
            name="data-input-init",
            image=DATA_SIDE_CAR_IMAGE,
            image_pull_policy="Always",
            command=["python3"],
            args=["init.py"],
            env=[
                client.V1EnvVar(name="INPUT_INIT_CONFIG", value="/opt/config/input-init.json"),
                client.V1EnvVar(name="DATA_DESTINATION", value="/data/"),
                client.V1EnvVar(name="CONFIG_FILE_PATH", value="/opt/config/workflow-api.cfg")
            ],

            volume_mounts=[
                client.V1VolumeMount(
                    mount_path="/opt/config/input-init.json",
                    sub_path="input-init.json",
                    name="input-init-config"
                ),
                client.V1VolumeMount(
                    mount_path="/opt/config/workflow-api.cfg",
                    sub_path="workflow-api.cfg",
                    name="workflow-api-config"
                ),
                client.V1VolumeMount(
                    mount_path="/data/",
                    name=JOB_VOLUME_NAME
                )
            ]
        )

        for resource in input_resources:
            if resource.type is ServiceResourceType.environment:
                continue

            sub_path = None
            mount_path = resource.mount_path

            if resource.type is ServiceResourceType.data:
                sub_path = resource.resource_name
                mount_path = os.path.join(resource.mount_path, resource.resource_name)

            worker_container_volume_mounts.append(
                client.V1VolumeMount(
                    mount_path=mount_path,
                    sub_path=sub_path,
                    name=JOB_VOLUME_NAME
                )
            )

        pod_spec_init_containers.append(data_init_container)

    if job_config.worker_image_output_directory:
        worker_container_volume_mounts.append(
            client.V1VolumeMount(
                mount_path=job_config.worker_image_output_directory,
                name=JOB_VOLUME_NAME
            )
        )

        side_car = client.V1Container(
            name="data-side-car",
            image=DATA_SIDE_CAR_IMAGE,
            image_pull_policy="Always"
        )

        side_car.volume_mounts = [
            client.V1VolumeMount(
                mount_path="/output",
                name=JOB_VOLUME_NAME)]

        containers.append(side_car)

    worker_container = client.V1Container(
        name="worker",
        image=job_config.worker_image,
        image_pull_policy="Always",
        command=job_config.worker_image_command,
        args=job_config.worker_image_args,
        volume_mounts=worker_container_volume_mounts
    )

    if job_config.gpu:
        worker_container.resources = client.V1ResourceRequirements(
            limits={"nvidia.com/gpu": "1"})

    if config_map_ref:
        env_from = [client.V1EnvFromSource(config_map_ref=client.V1ConfigMapEnvSource(name=ref))
                    for ref in config_map_ref]
        worker_container.env_from = env_from

    containers.append(worker_container)

    pod_spec = client.V1PodSpec(restart_policy="Never",
                                containers=containers,
                                image_pull_secrets=[
                                    client.V1LocalObjectReference(name=IMAGE_PULL_SECRET)],
                                init_containers=pod_spec_init_containers,
                                volumes=pod_spec_volumes)

    pod = client.V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=client.V1ObjectMeta(name=job_uuid,
                                     namespace=job_namespace,
                                     labels=labels),
        spec=pod_spec)
    return pod


def k8s_create_pod(manifest, namespace=NAMESPACE):
    return client.CoreV1Api().create_namespaced_pod(namespace=namespace,
                                                    body=manifest)


def k8s_delete_pod(name, namespace=NAMESPACE):

    client.CoreV1Api().delete_namespaced_pod(name=name,
                                             namespace=namespace)


def k8s_list_pod_names(namespace=NAMESPACE):

    pod_list = client.CoreV1Api().list_namespaced_pod(namespace=namespace)

    return [x.metadata.name for x in pod_list.items]


def k8s_create_service(name: str,
                       namespace: str,
                       job_id: str):

    service_body = client.V1Service(
        spec=client.V1ServiceSpec(
            type="NodePort",
            ports=[client.V1ServicePort(
                name="http",
                node_port=32000,
                port=9999,
                target_port=9999
            )],
            selector={"gx4ki-job-uuid": job_id}),
        metadata=client.V1ObjectMeta(name=name,
                                     labels={"gx4ki-app": "gx4ki-demo"}))
    client.CoreV1Api().create_namespaced_service(namespace=namespace,
                                                 body=service_body)


def k8s_delte_service(name: str,
                      namespace: str):

    client.CoreV1Api().delete_namespaced_service(
        name=name,
        namespace=namespace)


def k8s_create_config_map(data, name: str, namespace=NAMESPACE, labels=None):

    config_map = client.V1ConfigMap(data=data,
                                    metadata=client.V1ObjectMeta(
                                        name=name,
                                        namespace=namespace,
                                        labels=labels
                                    ))
    client.CoreV1Api().create_namespaced_config_map(body=config_map,
                                                    namespace=namespace)


def k8s_list_config_maps_names(namespace=NAMESPACE):
    config_maps = client.CoreV1Api().list_namespaced_config_map(namespace=namespace)

    return [x.metadata.name for x in config_maps.items]


def k8s_delete_config_map(name, namespace=NAMESPACE):

    try:
        client.CoreV1Api().delete_namespaced_config_map(name=name,
                                                        namespace=namespace)
    except ApiException as exc:
        print(exc)


def k8s_watch_pod_events(pod_name, pod_state_handle, namespace=NAMESPACE):
    # TODO currently unused in favor of k8s_get_job_info

    event_watch = watch.Watch()
    try:
        for event in event_watch.stream(
                client.CoreV1Api().list_namespaced_pod,
                namespace=namespace,
                field_selector=f"metadata.name={pod_name}"):

            def get_container_state(status):
                if status.running is not None:
                    return {"state": "running",
                            "details": status.running.to_str()}
                elif status.terminated is not None:
                    return {"state": "terminated",
                            "details": status.terminated.to_str()}
                else:
                    return {"state": "waiting",
                            "details": status.waiting.to_str()}

            container_states = None
            if event['object'].status.container_statuses is not None:
                container_states = dict()
                for x in event['object'].status.container_statuses:
                    container_states[x.name] = get_container_state(x.state)

            # TODO add pod state condition
            pod_state = K8sPodStateData(
                event_type=event['type'],
                pod_phase=event['object'].status.phase,
                pod_state_condition=[condition.to_str() for condition in event['object'].status.conditions],
                container_statuses=container_states)

            can_exit = pod_state_handle(pod_state)
            if can_exit:
                break

    finally:
        event_watch.stop()


@retry(max_retries=5)
def k8s_portforward(data, name, namespace=NAMESPACE) -> int:

    # Monkey patch socket.create_connection which is used by http.client and
    # urllib.request. The same can be done with urllib3.util.connection.create_connection
    # if the "requests" package is used.
    # socket_create_connection = socket.create_connection
    socket_create_connection = urllib3.util.connection.create_connection

    def kubernetes_create_connection(address, *args, **kwargs):
        dns_name = address[0]
        if isinstance(dns_name, bytes):
            dns_name = dns_name.decode()
        dns_name = dns_name.split(".")
        if dns_name[-1] != 'kubernetes':
            return socket_create_connection(address, *args, **kwargs)
        if len(dns_name) not in (3, 4):
            raise RuntimeError("Unexpected kubernetes DNS name.")
        namespace = dns_name[-2]
        name = dns_name[0]
        port = address[1]
        if len(dns_name) == 4:
            if dns_name[1] in ('svc', 'service'):
                service = client.CoreV1Api().read_namespaced_service(name, namespace)
                for service_port in service.spec.ports:
                    if service_port.port == port:
                        port = service_port.target_port
                        break
                else:
                    raise RuntimeError(
                        "Unable to find service port: %s" % port)
                label_selector = []
                for key, value in service.spec.selector.items():
                    label_selector.append("%s=%s" % (key, value))
                pods = client.CoreV1Api().list_namespaced_pod(
                    namespace, label_selector=",".join(label_selector)
                )
                if not pods.items:
                    raise RuntimeError("Unable to find service pods.")
                name = pods.items[0].metadata.name
                if isinstance(port, str):
                    for container in pods.items[0].spec.containers:
                        for container_port in container.ports:
                            if container_port.name == port:
                                port = container_port.container_port
                                break
                        else:
                            continue
                        break
                    else:
                        raise RuntimeError(
                            "Unable to find service port name: %s" % port)
            elif dns_name[1] != 'pod':
                raise RuntimeError(
                    "Unsupported resource type: %s" %
                    dns_name[1])
        pf = portforward(client.CoreV1Api().connect_get_namespaced_pod_portforward,
                         name, namespace, ports=str(port))
        return pf.socket(port)
    # socket.create_connection = kubernetes_create_connection

    urllib3.util.connection.create_connection = kubernetes_create_connection

    # Access the nginx http server using the
    # "<pod-name>.pod.<namespace>.kubernetes" dns name.
    response = requests.post(
        f"http://{name}.pod.{namespace}.kubernetes:9999/store",
        data=data
    )

    response.close()
    return response.status_code


def k8s_get_pod_log(pod_name: str,
                    container: str = None,
                    namespace: str = "default",
                    tail_lines: int = 100):
    """
    make request against k8s api to retrieve logs from the specified container
    """

    response = client.CoreV1Api().read_namespaced_pod_log(name=pod_name,
                                                          container=container,
                                                          namespace=namespace,
                                                          tail_lines=tail_lines)
    return response


def k8s_create_persistent_volume_claim(name: str,
                                       namespace: str,
                                       storage_size_in_Gi: str,
                                       labels=None):

    pvc_spec = client.V1PersistentVolumeClaimSpec(access_modes=["ReadWriteOnce"],
                                                  resources={"requests": {"storage": storage_size_in_Gi}})

    pvc_manifest = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels=labels),
        spec=pvc_spec
    )

    client.CoreV1Api().create_namespaced_persistent_volume_claim(body=pvc_manifest,
                                                                 namespace=namespace)


def k8s_delete_persistent_volume_claim(name,
                                       namespace):
    resonse = client.CoreV1Api().delete_namespaced_persistent_volume_claim(name=name,
                                                                           namespace=namespace)
    print(resonse)
