
import tempfile
import os
import uuid
import dotenv
from time import sleep
from fastapi import FastAPI, UploadFile
from fastapi.responses import JSONResponse, FileResponse

from starlette.background import BackgroundTask

from kubernetes import client, config
from kubernetes.stream import stream
from kubernetes.client.exceptions import ApiException


app = FastAPI()

WORKER_IMAGE = "harbor.gx4ki.imla.hs-offenburg.de/ralphhso/dummy-job:py3.8-alpine"
NAMESPACE = "gx4ki-demo"
RESULT_DIR = "/output"
KUBE_CONFIG_FILE = "kubeconfig/kubeconfig"
IMAGE_PULL_SECRET = "imla-registry"


def k8s_get_client() -> client.CoreV1Api:
    config.load_kube_config("kubeconfig/kubeconfig")
    client.Configuration()
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


def k8s_create_job_object(job_uuid, worker_image, config_map_ref="", job_namespace=NAMESPACE) -> client.V1Job:
    container = client.V1Container(
        name="dummy-job",
        image=worker_image,
        image_pull_policy="Always",
        args=["entrypoint.sh"],
        command=["/bin/bash"],
        # env=[
        #     client.V1EnvVar(name="SOURCE", value="/root/.bashrc"),
        #     client.V1EnvVar(name="DESTINATION", value="/output/.bashrc.backup")
        # ],
        volume_mounts=[
            client.V1VolumeMount(
                mount_path="/output",
                name="output-mount"
            )
        ]
    )

    if config_map_ref:
        env_from = [client.V1EnvFromSource(config_map_ref=client.V1ConfigMapEnvSource(name=ref))
                    for ref in config_map_ref.split(",") if ref]
        container.env_from = env_from

    side_car = client.V1Container(
        name="data-side-car",
        image="alpine",
        command=["/bin/sh", "-c",
                 "while true; do sleep 10; done"],
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
                    "gx4ki.job.uuid": job_uuid}),
        spec=client.V1PodSpec(restart_policy="Never",
                              containers=[container, side_car],
                              image_pull_secrets=[
                                  client.V1LocalObjectReference(name=IMAGE_PULL_SECRET)],
                              volumes=[client.V1Volume(
                                  name="output-mount",
                                  empty_dir=client.V1EmptyDirVolumeSource(size_limit="512M"))])
    )
    spec = client.V1JobSpec(
        template=template,
        backoff_limit=4
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_uuid),
        spec=spec,
    )
    return job


def k8s_create_job(job, namespace=NAMESPACE):
    return client.BatchV1Api().create_namespaced_job(body=job,
                                                     namespace=namespace)


def get_pod_name(job_uuid, namespace=NAMESPACE):
    job_completed = False
    # while not job_completed:
    pod_list = client.CoreV1Api().list_namespaced_pod(
        namespace=namespace,
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


def get_job_status(api_instance, namespace=NAMESPACE):
    job_completed = False

    while not job_completed:
        sleep(20)
        api_response = api_instance.read_namespaced_job_status(
            name="dummy",
            namespace=namespace)
        if api_response.status.succeeded is not None or \
                api_response.status.failed is not None:
            job_completed = True
        # sleep(20)
        print("Job status='%s'" % str(api_response))
        return


def update_job(api_instance, job, namespace=NAMESPACE):
    # Update container image
    job.spec.template.spec.containers[0].image = "perl"
    api_response = api_instance.patch_namespaced_job(
        name="dummy",
        namespace=namespace,
        body=job)
    print("Job updated. status='%s'" % str(api_response.status))


def delete_job(api_instance, namespace=NAMESPACE):
    api_response = api_instance.delete_namespaced_job(
        name="dummy",
        namespace=namespace,
        body=client.V1DeleteOptions(
            propagation_policy='Foreground',
            grace_period_seconds=5))
    print("Job deleted. status='%s'" % str(api_response.status))

    return api_response


def k8s_list_result_dir(pod_name, namespace=NAMESPACE):

    # list all file relative to /output/.*
    exec_command = ["/bin/sh", "-c", "find /output/ -type f | cut -d / -f 3-"]

    resp = stream(client.CoreV1Api().connect_get_namespaced_pod_exec, name=pod_name, namespace=namespace,
                  container='data-side-car', command=exec_command, stderr=True, stdin=False, stdout=True, tty=False)
    resp = [file for file in resp.split("\n") if file != ""]
    return resp


def k8s_read_file(pod_name, file_name, namespace=NAMESPACE):

    if not os.path.abspath(f"{RESULT_DIR}/{file_name}").startswith(RESULT_DIR):
        return None
    exec_command = ["/bin/sh", "-c", f"cat /output/{file_name}"]
    resp = stream(client.CoreV1Api().connect_get_namespaced_pod_exec, name=pod_name, namespace=namespace,
                  container='data-side-car', command=exec_command, stderr=True, stdin=False, stdout=True, tty=False)
    return resp


@app.on_event("startup")
async def startup():
    print("startup k8s client")
    if os.path.isfile(KUBE_CONFIG_FILE):
        config.load_kube_config(KUBE_CONFIG_FILE)
    # check if serviceaccount exits
    elif os.path.exists("/run/secrets/kubernetes.io/serviceaccount"):
        print("startup: try loading service account")
        config.load_incluster_config()
    else:
        raise Exception("FAIL: cannot connect to control plain")

    result = client.ApiClient().call_api(resource_path="/healthz",
                                         method="GET",
                                         #  query_params={"verbose": "true"},
                                         response_type=str)
    print(f"k8s/healthz: {result}")


@app.post("/job/deploy/")
async def deploy_job(job_conf = UploadFile,config_map_ref: str = ""):
    job_uuid = str(uuid.uuid4())

    job = k8s_create_job_object(job_uuid=job_uuid,
                                worker_image=WORKER_IMAGE,
                                config_map_ref=config_map_ref)
    try:
        resp = k8s_create_job(job)

    except ApiException as exception:
        return JSONResponse(content=exception.body, status_code=exception.status)
    except BaseException as exception:
        print(exception)
        return JSONResponse(status_code=500, content="Internal Server Error")

    # print(f"STATUS: {resp}")
    return JSONResponse(status_code=200, content={"status": "job deployed", "job_uuid": job_uuid})


def k8s_get_job_info(job_uuid, namespace=NAMESPACE):
    pod_list = client.CoreV1Api().list_namespaced_pod(namespace=namespace,
                                                      label_selector=f"gx4ki.job.uuid={job_uuid}")
    assert len(pod_list.items) == 1
    job_pod = pod_list.items[0]
    pod_info = {
        "pod_name": job_pod.metadata.name,
        "container_states": {container_status.name: container_status.state.to_dict()
                             for container_status in job_pod.status.container_statuses}
    }

    return pod_info


@app.get("/job/{job_uuid}", response_class=JSONResponse)
async def getJobInfo(job_uuid):
    pod_info = k8s_get_job_info(job_uuid)
    print(pod_info)
    return pod_info


def k8s_delete_job(name, namespace=NAMESPACE):

    status = client.BatchV1Api().delete_namespaced_job(name=name,
                                                       namespace=namespace,
                                                       orphan_dependents=True,
                                                       body=client.V1DeleteOptions(propagation_policy="Foreground",
                                                                                   grace_period_seconds=5))
    return status.to_dict()


def k8s_create_config_map(data, namespace=NAMESPACE):

    name = str(uuid.uuid4())

    config_map = client.V1ConfigMap(data=data,
                                    metadata=client.V1ObjectMeta(
                                        name=name,
                                        namespace=namespace,
                                        labels={"app": "gx4ki-demo"}
                                    ))
    client.CoreV1Api().create_namespaced_config_map(body=config_map,
                                                    namespace=namespace)
    return name


def k8s_delete_config_map(name, namespace=NAMESPACE):

    try:
        client.CoreV1Api().delete_namespaced_config_map(name=name,
                                                        namespace=namespace)
    except ApiException as exc:
        print(exc)


@app.delete("/job/{job_uuid}")
async def deleteJob(job_uuid):
    return k8s_delete_job(job_uuid)


@app.get("/job/{job_uuid}/result/list")
async def getJobData(job_uuid):
    pod_info = k8s_get_job_info(job_uuid=job_uuid)
    result = k8s_list_result_dir(pod_name=pod_info["pod_name"])
    print(result)
    return result


@app.get("/job/{job_uuid}/result")
async def getJobResultFile(job_uuid, file):
    pod_info = k8s_get_job_info(job_uuid=job_uuid)
    result = k8s_read_file(pod_name=pod_info["pod_name"], file_name=file)
    if result is None:
        return JSONResponse(content="illegal request", status_code=400)
    return result


def helper_get_env_data(env_file: UploadFile):
    # do we need to store that file?
    try:
        contents = env_file.file.read()
        with open(env_file.filename, 'wb') as f:
            f.write(contents)
    except Exception:
        return {"message": "There was an error uploading the file"}
    finally:
        env_file.file.close()

    data = dict(dotenv.dotenv_values(env_file.filename))
    os.remove(env_file.filename)
    return data


@app.post("/resource/env/")
async def postEnvFile(env_file: UploadFile):

    data = helper_get_env_data(env_file)
    config_map_id = k8s_create_config_map(data=data)

    return {"filename": env_file.filename, "resource_id": config_map_id}


@app.delete("/resource/env/{res_id}")
async def deleteEnvFile(res_id):
    k8s_delete_config_map(name=res_id)
    return {}


@app.get("/demo")
async def doDemoProto(env_file: UploadFile):
    job_id = str(uuid.uuid4())
    app.FORCE_QUIT = False
    job_completed = False

    print("CREATE CONFIG_MAP FROM ENV_FILE")
    data = helper_get_env_data(env_file)
    config_map_id = k8s_create_config_map(data)
    print(f"CONFIG_MAP CREATED config_map_id={config_map_id}")

    print(f"CREATE JOB MANIFEST AND DEPLOY: job_id={job_id}")
    job_manifest = k8s_create_job_object(
        job_uuid=job_id,
        worker_image=WORKER_IMAGE,
        config_map_ref=config_map_id)
    k8s_create_job(job_manifest)

    print("JOB DEPlOYED...WAIT FOR FINISH")
    sleep(5)
    job_info = k8s_get_job_info(job_id)
    while not app.FORCE_QUIT and not job_completed:
        job_info = k8s_get_job_info(job_id)
        if job_info["container_states"]["dummy-job"]['terminated'] is None:
            print("JOB RUNNING...")
            sleep(10)
        else:
            job_completed = True

    pod_info = k8s_get_job_info(job_uuid=job_id)
    print("SHOW RESULT FILES")
    data = k8s_list_result_dir(pod_name=pod_info["pod_name"])
    print(data)

    data = k8s_read_file(pod_name=pod_info["pod_name"],
                         file_name="result")

    print("WRITE DATA TO TMP")
    (tmp_fd, tmp_filename) = tempfile.mkstemp(prefix=f"{job_id}_")
    tmp_file = os.fdopen(tmp_fd, "w+")
    tmp_file.write(data)
    tmp_file.flush()
    tmp_file.close()

    print("DELETE JOB!")
    k8s_delete_job(job_id)

    return FileResponse(path=tmp_filename,
                        background=BackgroundTask(remove_tmp_file, tmp_filename))


def remove_tmp_file(tmp_filename):
    print("BACKGROUND_TASK DELETE FILE")
    os.remove(tmp_filename)


@app.post("/demo/forceQuit", status_code=200)
async def doDemoForceQuit():
    app.FORCE_QUIT = True


@app.get("/health")
async def health():
    return {'health': 'alive'}
