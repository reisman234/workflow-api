import unittest
from unittest.mock import MagicMock, patch

from middlelayer.models import ServiceResouce, ServiceResourceType, WorkflowResource
from middlelayer.backend import K8sWorkflowBackend, K8sJobData, Event

WORKFLOW_ID = "wf_id"

K8S_NAMESPACE = "test_ns"

K8S_CONFIGMAP_ID = "cm_id1"

SERVICE_RESOURCE = ServiceResouce(
    resource_name="test",
    type=ServiceResourceType.environment,
    description="test"
)
SERVICE_DATA = "data"

WORKFLOW_RESOURCE = WorkflowResource(
    worker_image="test_image",
    worker_image_output_directory="test_directory",
    gpu=True
)


class TestK8sWorkflowBackend_handleInput(unittest.TestCase):

    def setUp(self) -> None:

        self.job_data = K8sJobData(
            config_maps=[K8S_CONFIGMAP_ID],
            job_id="job_id",
            job_monitor_event=Event()
        )

    def tearDown(self) -> None:

        del self.job_data

    @patch('middlelayer.backend.k8s_create_config_map')
    def test_handle_input(self, mock_k8s_create_config_map: MagicMock):

        # setup
        mock_k8s_create_config_map.return_value = "config_map_id"
        testee = K8sWorkflowBackend(K8S_NAMESPACE)

        # exercise
        testee.handle_input(
            WORKFLOW_ID,
            SERVICE_RESOURCE,
            SERVICE_DATA)

        # verify
        self.assertEqual(
            len(testee.dummy_db.data[WORKFLOW_ID].config_maps), 1)

        mock_k8s_create_config_map.assert_called_once()
        mock_k8s_create_config_map.assert_called_with(
            data=SERVICE_DATA,
            namespace=K8S_NAMESPACE)

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

    @patch('middlelayer.backend.uuid4')
    @patch('middlelayer.backend.k8s_create_pod_manifest')
    @patch('middlelayer.backend.k8s_create_pod')
    @patch('middlelayer.backend.K8sWorkflowBackend._create_monitor_thread')
    def test_commit_workflow(self,
                             mock_create_monitor_thread: MagicMock,
                             mock_k8s_create_pod: MagicMock,
                             mock_k8s_create_pod_manifest: MagicMock,
                             mock_uuid4: MagicMock):

        # setup
        job_id = "job_id"
        job_manifest = "manifest"
        mock_k8s_create_pod_manifest.return_value = job_manifest
        mock_uuid4.return_value = job_id
        testee = K8sWorkflowBackend(K8S_NAMESPACE)
        testee.dummy_db.data[WORKFLOW_ID] = K8sJobData(config_maps=["test"])

        # exercise
        testee.commit_workflow(WORKFLOW_ID, WORKFLOW_RESOURCE)

        # verify
        mock_uuid4.assert_called_once()
        self.assertIsNotNone(testee.dummy_db.data.get(WORKFLOW_ID))
        self.assertEqual(testee.dummy_db.data.get(WORKFLOW_ID).job_id, job_id)

        mock_k8s_create_pod_manifest.assert_called_once_with(
            job_uuid=job_id,
            job_config=WORKFLOW_RESOURCE,
            job_namespace=K8S_NAMESPACE)

        mock_k8s_create_pod.assert_called_once_with(
            manifest=job_manifest,
            namespace=K8S_NAMESPACE)

        mock_create_monitor_thread.assert_called_once()

    @patch('middlelayer.backend.Thread')
    def test_create_monitor_thread(self, mock_thread: MagicMock):

        # setup
        job_id = "test"
        testee = K8sWorkflowBackend(K8S_NAMESPACE)
        testee.dummy_db.data[WORKFLOW_ID] = K8sJobData()

        # exercise
        testee._create_monitor_thread(WORKFLOW_ID, job_id)

        # verify
        self.assertIsNotNone(testee.dummy_db.data.get(
            WORKFLOW_ID).job_monitor_event)

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
