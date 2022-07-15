import socket
import uuid
from fastapi.responses import StreamingResponse, PlainTextResponse
from threading import Event
from time import sleep
from kubernetes import client, config
from kubernetes.stream import stream
from fastapi import FastAPI, Request


app = FastAPI()


def k8s_get_client() -> client.CoreV1Api:
    config.load_kube_config("kubeconfig/kubeconfig")
    return client.CoreV1Api()


async def list_pods():

    v1 = k8s_get_client()
    ret = v1.list_pod_for_all_namespaces(watch=False)

    print("Listing pods with their IPs:")
    result = {}
    for i in ret.items:
        print(f"{i.status.pod_ip}\t{i.metadata.namespace}\t{i.metadata.name}")
        result[i.metadata.name] = {
            "ip": i.status.pod_ip, "namespace": i.metadata.namespace}
    return result


def create_job_object(job_uuid) -> client.V1Job:
    container = client.V1Container(
        name="dummy-job",
        image="ralphhso/dummy-job:latest",
        image_pull_policy="Never",
        args=["entrypoint.sh"],
        command=["/bin/bash"],
        # "-c", "while true; do sleep 10; done"],
        # "apt-get update && apt-get install-y curl && curl 192.168.49.2:8888/k8s/jobstop"],
        env=[
            client.V1EnvVar(name="SOURCE", value="/root/.bashrc"),
            client.V1EnvVar(name="DESTINATION", value="/output/.bashrc.backup")
        ],
        volume_mounts=[
            client.V1VolumeMount(
                mount_path="/output",
                name="output-mount"
            )
        ]
    )
    side_car = client.V1Container(
        name="dummy-side-car",
        image="alpine",
        command=["/bin/sh", "-c", "while true; do sleep 10; done"],
        volume_mounts=[
            client.V1VolumeMount(
                mount_path="/output",
                name="output-mount"
            )
        ]
    )
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(
            labels={"job_name": "dummy",
                    "myjob": "dummy",
                    "gx4ki.job.uuid": job_uuid}),
        spec=client.V1PodSpec(restart_policy="Never",
                              containers=[container, side_car],
                              volumes=[client.V1Volume(
                                  name="output-mount",
                                  empty_dir=client.V1EmptyDirVolumeSource(size_limit="512M"))])
    )
    spec = client.V1JobSpec(
        template=template,
        backoff_limit=4
    )
    # volumes=[client.V1Volume(
    # name="output-mount",
    # empty_dir=client.V1EmptyDirVolumeSource(size_limit="512M"))])
    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name="dummy"),
        spec=spec,
    )
    return job


def create_job(api_instance, job):
    api_response = api_instance.create_namespaced_job(
        body=job,
        namespace="default")
    print("Job created. status='%s'" % str(api_response.status))


def get_pod_name(job_uuid):
    job_completed = False
    while not job_completed:
        pod_list = client.CoreV1Api().list_namespaced_pod(
            namespace="default",
            label_selector=f"gx4ki.job.uuid={job_uuid}",
            field_selector="status.phase=Running"
        )
        # print(pod_list)
        for pod in pod_list.items:
            print("pod:")
            print(f"  name: {pod.metadata.name}")
            print(f"  status.phase: {pod.status.phase}")
            if pod.status.container_statuses is not None:
                # print(
                #     f"    status.container_statuses: {pod.status.container_statuses}")
                if pod.status.container_statuses[0].state.terminated is not None:
                    print(
                        f"    status.container_statuses[0].state.terminated.reason: \
                            {pod.status.container_statuses[0].state.terminated.reason}")
                    print(
                        f"    status.container_statuses[0].state.terminated.exit_code: \
                            {pod.status.container_statuses[0].state.terminated.exit_code}")
                    if pod.status.container_statuses[0].state.terminated.exit_code == 0:
                        job_completed = True
                        pod_name = pod.metadata.name
    return pod_name


def get_job_status(api_instance):
    job_completed = False

    while not job_completed:
        sleep(20)
        api_response = api_instance.read_namespaced_job_status(
            name="dummy",
            namespace="default")
        if api_response.status.succeeded is not None or \
                api_response.status.failed is not None:
            job_completed = True
        # sleep(20)
        print("Job status='%s'" % str(api_response))
        return


def update_job(api_instance, job):
    # Update container image
    job.spec.template.spec.containers[0].image = "perl"
    api_response = api_instance.patch_namespaced_job(
        name="dummy",
        namespace="default",
        body=job)
    print("Job updated. status='%s'" % str(api_response.status))


def delete_job(api_instance):
    api_response = api_instance.delete_namespaced_job(
        name="dummy",
        namespace="default",
        body=client.V1DeleteOptions(
            propagation_policy='Foreground',
            grace_period_seconds=5))
    print("Job deleted. status='%s'" % str(api_response.status))

    return api_response


def ls_output_dir(api_instance: client.CoreV1Api, pod_name):

    exec_command = ["/bin/sh", "-c", "ls -al /tmp"]
    # resp = stream(api_instance.connect_get_namespaced_pod_exe,
    #               pod_name, 'default',
    #               container="dummy-side-car",
    #               command=exec_command,
    #               stderr=True, stdin=True,
    #               stdout=True, tty=False,
    #               _preload_content=False)

    resp = stream(api_instance.connect_get_namespaced_pod_exec, pod_name, 'default',
                  container='dummy-side-car', command=exec_command, stderr=True, stdin=False, stdout=True, tty=False)

    print(resp)
    return resp


class Dummy:

    def __init__(self):
        self.data = None
        self.lock = Event()
        self.buffer = None

    def set_data(self, obj):
        self.buffer = obj
        self.lock.set()

    def get_data(self):
        self.lock.wait()
        return self.buffer


data_share = Dummy()


@app.get("/", response_class=PlainTextResponse)
async def root(request: Request):
    print("request data from provider")
    print(await request.body())
    result = await list_pods()

    job_uuid = str(uuid.uuid4())
    config.load_kube_config("kubeconfig/kubeconfig")
    v1 = client.BatchV1Api()
    job = create_job_object(job_uuid=job_uuid)
    create_job(v1, job)
    pod_name = get_pod_name(job_uuid=job_uuid)

    if pod_name is None:
        print("ERROR: NO POD_NAME... CANNOT EXEC")
    else:
        result = ls_output_dir(client.CoreV1Api(), pod_name)

    # data = await wait_for_data()

    return result
    # return StreamingResponse(wait_for_data())  # yield_data()


async def yield_data():
    for i in range(10):
        sleep(1)
        yield b"fake data\n"


@app.post("/k8s/jobstop", status_code=200)
async def k8s_preStopHook(request: Request):
    print("REQUEST FROM JOB")
    print(await request.body())


@app.post("/data")
async def get_data(request: Request):
    data = await request.body()
    print(f"{data}")
    print(data_share)
    data_share.set_data(await request.body())
    return data


async def get_result_data():

    print("wait for result")
    print(data_share)
    return data_share.get_data()


async def wait_for_data():
    HOST = "192.168.49.5"
    PORT = 40001
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        conn, addr = s.accept()

        with conn:
            print(f"CONNECTED by {addr}")
            while True:
                data = conn.recv(1024)
                data = data.decode('utf8')
                print(data)
                yield data
                if not data:
                    break
