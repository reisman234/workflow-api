from time import sleep
from unittest import TestCase
from middlelayer.k8sClient import config
from middlelayer.k8sClient import k8s_setup_config, k8s_get_healthz,\
    k8s_create_config_map, k8s_delete_config_map, k8s_list_config_maps_names, \
    k8s_create_pod_manifest, k8s_create_pod, k8s_delete_pod, k8s_list_pod_names,\
    k8s_create_service, k8s_delte_service


from middlelayer.backend import WorkflowResource


class TestK8sClient(TestCase):

    def setUp(self) -> None:
        # loads $
        k8s_setup_config()
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
