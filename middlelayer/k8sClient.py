from typing import List, Dict, Union

import urllib3
import requests

from kubernetes import client, config, watch
from kubernetes.stream import portforward
from kubernetes.client.exceptions import ApiException


from middlelayer.models import WorkflowResource, BaseModel


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


def k8s_setup_config(config_file=None, image_pull_secret=None):
    if config_file:
        # load a specific config_file
        config.load_kube_config(config_file=config_file)
    else:
        config.load_config()

    if image_pull_secret:
        global IMAGE_PULL_SECRET
        IMAGE_PULL_SECRET = image_pull_secret


def k8s_get_healthz():
    return client.ApiClient().call_api(resource_path="/healthz",
                                       method="GET",
                                       #  query_params={"verbose": "true"},
                                       response_type=str)


def k8s_create_pod_manifest(job_uuid,
                            job_config: WorkflowResource,
                            config_map_ref: List[str] = None,
                            job_namespace=NAMESPACE,
                            labels=None) -> client.V1Pod:
    container = client.V1Container(
        name="worker",
        image=job_config.worker_image,
        image_pull_policy="IfNotPresent",
        # args=["entrypoint.sh"],
        # command=["/bin/bash"],
    )

    if job_config.worker_image_output_directory:
        container.volume_mounts = [
            client.V1VolumeMount(
                mount_path=job_config.worker_image_output_directory,
                name="output-mount"
            )
        ]

    if job_config.gpu:
        container.resources = client.V1ResourceRequirements(
            limits={"nvidia.com/gpu": "1"})

    if config_map_ref:
        env_from = [client.V1EnvFromSource(config_map_ref=client.V1ConfigMapEnvSource(name=ref))
                    for ref in config_map_ref]
        container.env_from = env_from

    side_car = client.V1Container(
        name="data-side-car",
        image="harbor.gx4ki.imla.hs-offenburg.de/gx4ki/data-side-car:latest",
        # command=["/bin/sh"],
        # tty=True,
        image_pull_policy="Never",  # TODO REMOVE
    )

    if job_config.worker_image_output_directory:
        side_car.volume_mounts = [
            client.V1VolumeMount(
                mount_path="/output",
                name="output-mount")]

    pod_spec = client.V1PodSpec(restart_policy="Never",
                                containers=[container, side_car],
                                image_pull_secrets=[
                                    client.V1LocalObjectReference(name=IMAGE_PULL_SECRET)],
                                volumes=[client.V1Volume(
                                    name="output-mount",
                                    empty_dir=client.V1EmptyDirVolumeSource(size_limit="2Gi"))])

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
