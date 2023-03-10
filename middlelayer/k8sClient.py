import uuid
from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException
from middlelayer.models import WorkflowResource

from kubernetes.client import V1ConfigMapList

NAMESPACE = "default"
IMAGE_PULL_SECRET = "imla-registry"


def k8s_setup_config(config_file=None):
    # if os.path.isfile(KUBE_CONFIG_FILE):
    config.load_kube_config(config_file=config_file)

    # check if serviceaccount exits
    # elif os.path.exists("/run/secrets/kubernetes.io/serviceaccount"):
    #     print("startup: try loading service account")
    #     config.load_incluster_config()
    # else:
    #     raise Exception("FAIL: cannot connect to control plain")


def k8s_get_healthz():
    return client.ApiClient().call_api(resource_path="/healthz",
                                       method="GET",
                                       #  query_params={"verbose": "true"},
                                       response_type=str)


def k8s_create_pod_manifest(job_uuid,
                            job_config: WorkflowResource,
                            config_map_ref="",
                            job_namespace=NAMESPACE) -> client.V1Pod:
    container = client.V1Container(
        name="worker",
        image=job_config.worker_image,
        image_pull_policy="Always",
        args=["entrypoint.sh"],
        command=["/bin/bash"],
        volume_mounts=[
            client.V1VolumeMount(
                mount_path=job_config.worker_image_output_directory,
                name="output-mount"
            )
        ]
    )

    if job_config.gpu:
        container.resources = client.V1ResourceRequirements(
            limits={"nvidia.com/gpu": "1"})

    if config_map_ref:
        env_from = [client.V1EnvFromSource(config_map_ref=client.V1ConfigMapEnvSource(name=ref))
                    for ref in config_map_ref.split(",") if ref]
        container.env_from = env_from

    side_car = client.V1Container(
        name="data-side-car",
        image="harbor.gx4ki.imla.hs-offenburg.de/gx4ki/imla-data-side-car:latest",
        command=["/bin/sh"],
        tty=True,
        volume_mounts=[
            client.V1VolumeMount(
                mount_path="/output",
                name="output-mount"
            )
        ]
    )

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
                                     labels={"gx4ki-app": "gx4ki-demo",
                                             "gx4ki-job-uuid": job_uuid}),
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


def k8s_get_job_info(job_uuid, namespace=NAMESPACE):
    pod_list = client.CoreV1Api().list_namespaced_pod(namespace=namespace,
                                                      label_selector=f"gx4ki.job.uuid={job_uuid}")

    assert len(pod_list.items) == 1
    job_pod = client.V1Pod(pod_list.items[0])
    pod_info = {"pod_name": job_pod.metadata.name}

    pod_info["pod.status.phase"] = job_pod.status.phase

    pod_info["container_states"] = None
    if job_pod.status.container_statuses:
        pod_info["container_states"] = {container_status.name: container_status.state.to_dict()
                                        for container_status in job_pod.status.container_statuses}

    return pod_info


def k8s_watch_pod_events(pod_name, namespace=NAMESPACE):
    # TODO currently unused in favor of k8s_get_job_info
    w = watch.Watch()
    for event in w.stream(client.CoreV1Api().list_namespaced_pod, namespace=namespace, field_selector=f"metadata.name={pod_name}"):
        # Print the event type and the pod's new status
        def get_container_state(status):
            if status.running is not None:
                return f"running, details: {status.running}"
            elif status.terminated is not None:
                return f"terminated, details: {status.terminated}"
            else:
                return f"waiting, details: {status.waiting}"
        container_states = "unknown"
        if event['object'].status.container_statuses is not None:
            container_states = [(x.name, get_container_state(x.state))
                                for x in event['object'].status.container_statuses]
        print(
            f"Event type: {event['type']}, Pod status: {event['object'].status.phase}, ContainerStatus: {container_states}")
