from time import sleep
from threading import Thread
from unittest import TestCase
from middlelayer.k8sClient import K8sPodStateData
from middlelayer.k8sClient import k8s_setup_config, k8s_get_healthz,\
    k8s_create_config_map, k8s_delete_config_map, k8s_list_config_maps_names, \
    k8s_create_pod_manifest, k8s_create_pod, k8s_delete_pod, k8s_list_pod_names,\
    k8s_create_service, k8s_delte_service,\
    k8s_watch_pod_events, k8s_get_pod_log,\
    k8s_portforward


from middlelayer.backend import WorkflowResource

POD_NAMESPACE = "default"
POD_NAME = "test-pod"
POD_MANIFEST = {
    "kind": "Pod",
    "apiVersion": "v1",
    "metadata": {
        "name": POD_NAME,
        "labels": {
            "app": POD_NAME
        }
    },
    "spec": {
        "containers": [
            {
                "name": "test-pod",
                "image": "ubuntu:22.04",
                "command": ["/bin/bash"],
                "tty": True,
                "stdin": True,
                "resources": {
                    "limits": {"cpu": 1, "memory": "512Mi"}
                }
            }
        ],
        "restartPolicy": "Always",
        "dnsPolicy": "ClusterFirst"
    }
}


class TestK8sClient(TestCase):

    def setUp(self) -> None:
        # loads $
        k8s_setup_config(config_file="/home/ralph/.kube/minikube.config")
        self.test_config_map_name = "test-cm"

        self.workflow_resource = WorkflowResource(
            worker_image="ubuntu:20.04",
            gpu=False,
            worker_image_output_directory="/output/"
        )

    def tearDown(self) -> None:
        return super().tearDown()

    def test_get_healthz(self):

        # exercise
        result = k8s_get_healthz()

        # verify
        self.assertIn("ok", result)

    def test_create_and_delete_config_map(self):

        # setup
        data = {"DATA": "TEST"}

        # exercise
        k8s_create_config_map(
            data=data,
            name=self.test_config_map_name,
            labels={"app": "TestK8sClient"})

        # verify
        config_map_names = k8s_list_config_maps_names()
        self.assertIn(self.test_config_map_name, config_map_names)

        # cleanup
        k8s_delete_config_map(
            name=self.test_config_map_name)

    def test_create_and_delete_pod(self):

        # setup
        job_id = "test-job-id"
        worker_image_args = [
            "-c",
            "for i in $(seq 1 5); do sleep 1; echo $i; done"
        ]

        pod_manifest = k8s_create_pod_manifest(
            job_uuid=job_id,
            job_config=self.workflow_resource)

        pod_manifest.spec.containers[0].args = worker_image_args

        self.assertEqual(job_id, pod_manifest.metadata.name)
        self.assertEqual(worker_image_args,
                         pod_manifest.spec.containers[0].args)

        # change image of data-side-car
        pod_manifest.spec.containers[1].image = "ubuntu:20.04"
        pod_manifest.spec.containers[1].args = worker_image_args
        k8s_create_pod(pod_manifest)

        sleep(15)
        k8s_delete_pod(
            name=job_id)

    def test_create_side_car_service(self):

        # TODO make test standalone

        k8s_create_service(name="my-pod-service",
                           namespace="gx4ki-demo",
                           job_id="my-pod")

    def test_delete_side_car_service(self):

        # TODO make test standalone

        k8s_delte_service(name="my-pod-service",
                          namespace="gx4ki-demo")

    def test_list_pods(self):

        k8s_list_pod_names()

    def test_portford(self):
        pass
        # TODO make test standalone

        k8s_portforward(
            data="test",
            name="TODO",
            namespace="gx4ki-demo")

    def test_watch_pod_events(self):

        k8s_create_pod(POD_MANIFEST)

        def pod_state_handle(pod_state: K8sPodStateData):
            print(pod_state)

            if pod_state.container_statuses is None:
                return False
            if pod_state.container_statuses[POD_NAME].state == "terminated":
                return True

            return False

        event_thread = Thread(target=k8s_watch_pod_events,
                              args=[POD_NAME, pod_state_handle, POD_NAMESPACE])
        event_thread.start()

        sleep(10)
        k8s_delete_pod(POD_NAME)

        event_thread.join()

    def test_get_pod_log(self):

        logs = k8s_get_pod_log(
            pod_name="worker-5b64886fd5-w5xsq", tail_lines=None)

        print(logs)
        self.assertIsInstance(logs, str)
