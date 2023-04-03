import unittest
import dotenv
from io import StringIO
from unittest.mock import MagicMock, patch

from middlelayer.models import ServiceResouce, ServiceResourceType, WorkflowResource, WorkflowStoreInfo, MinioStoreInfo
from middlelayer.backend import K8sWorkflowBackend, K8sJobData, Event

WORKFLOW_ID = "wf_id"

K8S_NAMESPACE = "test_ns"

K8S_CONFIGMAP_ID = "cm_id1"

SERVICE_RESOURCE = ServiceResouce(
    resource_name="test",
    type=ServiceResourceType.environment,
    description="test"
)
SERVICE_DOTENV_DATA = "data=data"

WORKFLOW_RESOURCE = WorkflowResource(
    worker_image="test_image",
    worker_image_output_directory="test_directory",
    gpu=True
)


class TestK8sWorkflowBackend_handleInput(unittest.TestCase):

    def setUp(self) -> None:
        self.job_id = "test_job_id"
        self.workflow_id = "test_workflow_id"
        self.k8s_namespace = "test_namespace"
        self.testee = K8sWorkflowBackend(self.k8s_namespace)

        self.job_data = K8sJobData(
            config_maps=[K8S_CONFIGMAP_ID],
            job_id=self.job_id,
            job_monitor_event=Event()
        )

    def tearDown(self) -> None:
        pass
        # del self.job_data

    @patch('middlelayer.backend.k8s_create_config_map')
    def test_handle_input(self, mock_k8s_create_config_map: MagicMock):

        # setup
        mock_get_data_handle = MagicMock()
        mock_get_data_handle.return_value = SERVICE_DOTENV_DATA
        testee = K8sWorkflowBackend(K8S_NAMESPACE)

        config_map_id = K8S_CONFIGMAP_ID

        with patch("middlelayer.backend.uuid4", return_value=K8S_CONFIGMAP_ID):
            # exercise
            testee.handle_input(
                WORKFLOW_ID,
                SERVICE_RESOURCE,
                mock_get_data_handle)

            # verify
            self.assertEqual(
                len(testee.dummy_db.data[WORKFLOW_ID].config_maps), 1)

            mock_k8s_create_config_map.assert_called_once()
            mock_k8s_create_config_map.assert_called_with(
                name=K8S_CONFIGMAP_ID,
                namespace=K8S_NAMESPACE,
                data=dict(dotenv.dotenv_values(
                    stream=StringIO(SERVICE_DOTENV_DATA))))

    @patch('middlelayer.backend.k8s_delete_config_map')
    @patch('middlelayer.backend.k8s_delete_pod')
    def test_cleanup(self,
                     mock_k8s_delete_pod: MagicMock,
                     mock_delete_config_map: MagicMock):

        # setup
        testee = K8sWorkflowBackend(K8S_NAMESPACE)

        # exercise
        testee.cleanup(WORKFLOW_ID)
        mock_delete_config_map.assert_not_called()

        testee.dummy_db.data[WORKFLOW_ID] = self.job_data

        testee.cleanup(WORKFLOW_ID)
        # verify
        mock_delete_config_map.assert_called_once()
        mock_delete_config_map.assert_called_with(
            K8S_CONFIGMAP_ID,
            K8S_NAMESPACE)

        mock_k8s_delete_pod.assert_called_once_with(
            name=self.job_data.job_id,
            namespace=K8S_NAMESPACE)

        self.assertTrue(self.job_data.job_monitor_event.is_set())
        self.assertIsNone(testee.dummy_db.get_job_data(WORKFLOW_ID))

    def test_commit_workflow(self):
        # setup
        job_id = "job_id"
        job_manifest = "manifest"

        with patch("middlelayer.backend.uuid4", return_value=job_id) as mock_uuid4,\
                patch("middlelayer.backend.k8s_create_pod_manifest") as mock_k8s_create_pod_manifest,\
                patch('middlelayer.backend.k8s_create_pod') as mock_k8s_create_pod,\
                patch("middlelayer.backend.Event") as mock_event,\
                patch("middlelayer.backend.Thread") as mock_thread:

            mock_thread_instance = mock_thread.return_value
            mock_event_instance = mock_event.return_value

            mock_workflow_finished_handle = MagicMock()

            mock_k8s_create_pod_manifest.return_value = job_manifest
            testee = K8sWorkflowBackend(K8S_NAMESPACE)
            # testee.dummy_db.data[WORKFLOW_ID] = K8sJobData(
            # config_maps=["test"])

            # exercise
            testee.commit_workflow(
                workflow_id=WORKFLOW_ID,
                workflow_resource=WORKFLOW_RESOURCE,
                workflow_finished_handle=mock_workflow_finished_handle)

            # verify
            mock_uuid4.assert_called_once()
            # self.assertIsNotNone(testee.dummy_db.data.get(WORKFLOW_ID))
            # self.assertEqual(testee.dummy_db.data.get(
            #     WORKFLOW_ID).job_id, job_id)

            mock_k8s_create_pod_manifest.assert_called_once_with(
                job_uuid=job_id,
                job_config=WORKFLOW_RESOURCE,
                config_map_ref=[],
                job_namespace=K8S_NAMESPACE)

            mock_k8s_create_pod.assert_called_once_with(
                manifest=job_manifest,
                namespace=K8S_NAMESPACE)

            mock_thread.assert_called_once()
            mock_thread_instance.start.assert_called_once()

            mock_event.assert_called_once()
            assert testee.dummy_db.get_job_monitor_event(
                WORKFLOW_ID) == mock_event_instance

    def test_commit_workflow_with_config_maps(self):
        # setup
        job_id = "job_id"
        job_manifest = "manifest"

        with patch("middlelayer.backend.uuid4", return_value=job_id) as mock_uuid4,\
                patch("middlelayer.backend.k8s_create_pod_manifest") as mock_k8s_create_pod_manifest,\
                patch('middlelayer.backend.k8s_create_pod') as mock_k8s_create_pod,\
                patch("middlelayer.backend.Event") as mock_event,\
                patch("middlelayer.backend.Thread") as mock_thread:

            mock_thread_instance = mock_thread.return_value
            mock_event_instance = mock_event.return_value

            mock_workflow_finished_handle = MagicMock()

            mock_k8s_create_pod_manifest.return_value = job_manifest
            testee = K8sWorkflowBackend(K8S_NAMESPACE)
            testee.dummy_db.append_config_map(WORKFLOW_ID, K8S_CONFIGMAP_ID)

            # exercise
            testee.commit_workflow(
                workflow_id=WORKFLOW_ID,
                workflow_resource=WORKFLOW_RESOURCE,
                workflow_finished_handle=mock_workflow_finished_handle)

            # verify
            mock_uuid4.assert_called_once()

            mock_k8s_create_pod_manifest.assert_called_once_with(
                job_uuid=job_id,
                job_config=WORKFLOW_RESOURCE,
                config_map_ref=[K8S_CONFIGMAP_ID],
                job_namespace=K8S_NAMESPACE)

            mock_k8s_create_pod.assert_called_once_with(
                manifest=job_manifest,
                namespace=K8S_NAMESPACE)

            mock_thread.assert_called_once()
            mock_thread_instance.start.assert_called_once()

            mock_event.assert_called_once()
            assert testee.dummy_db.get_job_monitor_event(
                WORKFLOW_ID) == mock_event_instance

    def test_cleanup_monitor_thread(self):

        # setup
        job_id = "test"
        testee = K8sWorkflowBackend(K8S_NAMESPACE)
        testee.dummy_db.data[WORKFLOW_ID] = K8sJobData(
            job_monitor_event=Event())

        # exercise
        testee._cleanup_monitor(WORKFLOW_ID)

        # verify
        self.assertTrue(
            testee.dummy_db.data[WORKFLOW_ID].job_monitor_event.is_set())

    def test_store_result(self):

        # setup

        workflow_id = "fake_workeflow_id"
        workflow_store_info = WorkflowStoreInfo(
            minio=MinioStoreInfo(
                endpoint="fake",
                access_key="fake_access_id",
                secret_key="fake_access_secret",
                secure=True),
            destination_bucket="fake_bucket",
            destination_path="fake_path",
            result_directory="fake_directory",
            result_files=["fake_file"])

        with self.assertRaises(KeyError):
            self.testee.store_result(
                workflow_id=workflow_id,
                workflow_store_info=workflow_store_info)

        workflow_id = self.workflow_id
        self.testee.dummy_db.data[self.workflow_id] = self.job_data

        with patch("middlelayer.backend.requests") as mock_requests:

            self.testee.store_result(
                workflow_id=workflow_id,
                workflow_store_info=workflow_store_info)

        mock_requests.post.assert_called_once_with(
            url=f"http://{self.job_id}/store/",
            data=workflow_store_info
        )

    def test_monitor_workflow(self):

        # setup
        testee = K8sWorkflowBackend(K8S_NAMESPACE)
        mock_event = MagicMock()
        mock_handle = MagicMock()

        test_fail_container_state = {"container_states": {
            "dummy-job": {"waiting": {"reason": "ErrImagePull"}}},
            "pod.status.phase": "Pending"}

        mock_event.is_set.return_value = False

        # exercise
        with patch("middlelayer.backend.k8s_get_job_info", return_value=test_fail_container_state),\
                patch("middlelayer.backend.sleep"),\
                self.assertRaises(Exception):

            testee.monitor_workflow(
                self.job_id,
                mock_event,
                mock_handle)

        job_info_side_effect = [
            {"container_states": {
                "dummy-job": {"terminated": None}},
                "pod.status.phase": "Running"},
            {"container_states": {
                "dummy-job": {"terminated": {"fake": "fake"}}},
                "pod.status.phase": "Running"}
        ]

        # exercise
        with patch("middlelayer.backend.k8s_get_job_info", side_effect=job_info_side_effect),\
                patch("middlelayer.backend.sleep"):

            testee.monitor_workflow(
                self.job_id,
                mock_event,
                mock_handle)

        mock_event.set.assert_called_once()
        mock_handle.assert_called_once()
