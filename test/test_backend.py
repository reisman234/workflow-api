import unittest
import dotenv
from io import StringIO
from unittest.mock import MagicMock, patch

from middlelayer.models import ServiceResouce, ServiceResourceType, WorkflowResource, WorkflowStoreInfo, MinioStoreInfo
from middlelayer.backend import K8sWorkflowBackend, K8sJobData, Event, WorkflowJobState

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
        with patch("middlelayer.backend.k8s_setup_config"):
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
        # testee = K8sWorkflowBackend(K8S_NAMESPACE)

        with patch("middlelayer.backend.uuid4", return_value=K8S_CONFIGMAP_ID):
            # exercise
            self.testee.handle_input(
                self.workflow_id,
                SERVICE_RESOURCE,
                mock_get_data_handle)

            # verify
            self.assertEqual(
                len(self.testee.dummy_db.data[self.workflow_id].config_maps), 1)

            mock_k8s_create_config_map.assert_called_once()
            mock_k8s_create_config_map.assert_called_with(
                name=K8S_CONFIGMAP_ID,
                namespace=self.k8s_namespace,
                data=dict(dotenv.dotenv_values(
                    stream=StringIO(SERVICE_DOTENV_DATA))),
                labels={"app": "gx4ki-demo",
                        "workflow-id": self.workflow_id})

    @patch('middlelayer.backend.k8s_delete_config_map')
    @patch('middlelayer.backend.k8s_delete_pod')
    def test_cleanup(self,
                     mock_k8s_delete_pod: MagicMock,
                     mock_delete_config_map: MagicMock):

        # exercise
        self.testee.cleanup(WORKFLOW_ID)
        mock_delete_config_map.assert_not_called()

        self.testee.dummy_db.data[WORKFLOW_ID] = self.job_data

        self.testee.cleanup(WORKFLOW_ID)
        # verify
        mock_delete_config_map.assert_called_once()
        mock_delete_config_map.assert_called_with(
            K8S_CONFIGMAP_ID,
            self.k8s_namespace)

        mock_k8s_delete_pod.assert_called_once_with(
            name=self.job_data.job_id,
            namespace=self.k8s_namespace)

        self.assertTrue(self.job_data.job_monitor_event.is_set())
        self.assertIsNotNone(self.testee.dummy_db.get_job_data(WORKFLOW_ID))

    def test_commit_workflow(self):
        # setup
        job_manifest = "manifest"

        with patch("middlelayer.backend.uuid4", return_value=self.job_id) as mock_uuid4,\
                patch("middlelayer.backend.k8s_create_pod_manifest") as mock_k8s_create_pod_manifest,\
                patch('middlelayer.backend.k8s_create_pod') as mock_k8s_create_pod,\
                patch("middlelayer.backend.Event") as mock_event,\
                patch("middlelayer.backend.Thread") as mock_thread:

            mock_thread_instance = mock_thread.return_value
            mock_event_instance = mock_event.return_value

            mock_workflow_finished_handle = MagicMock()

            mock_k8s_create_pod_manifest.return_value = job_manifest

            # exercise
            self.testee.commit_workflow(
                workflow_id=self.workflow_id,
                workflow_resource=WORKFLOW_RESOURCE,
                workflow_finished_handle=mock_workflow_finished_handle)

            # verify
            mock_uuid4.assert_called_once()

            mock_k8s_create_pod_manifest.assert_called_once_with(
                job_uuid=self.job_id,
                job_config=WORKFLOW_RESOURCE,
                config_map_ref=[],
                input_config_ref=None,
                input_resources=None,
                job_namespace=self.k8s_namespace,
                labels={"app": "gx4ki-demo",
                        "workflow-id": self.workflow_id,
                        "job-id": self.job_id})

            mock_k8s_create_pod.assert_called_once_with(
                manifest=job_manifest,
                namespace=self.k8s_namespace)

            mock_thread.assert_called_once()
            mock_thread_instance.start.assert_called_once()

            mock_event.assert_called_once()
            assert self.testee.dummy_db.get_job_monitor_event(
                self.workflow_id) == mock_event_instance

    def test_commit_workflow_with_config_maps(self):
        # setup
        job_manifest = "manifest"

        with patch("middlelayer.backend.uuid4", return_value=self.job_id) as mock_uuid4,\
                patch("middlelayer.backend.k8s_create_pod_manifest") as mock_k8s_create_pod_manifest,\
                patch('middlelayer.backend.k8s_create_pod') as mock_k8s_create_pod,\
                patch("middlelayer.backend.Event") as mock_event,\
                patch("middlelayer.backend.Thread") as mock_thread:

            mock_thread_instance = mock_thread.return_value
            mock_event_instance = mock_event.return_value

            mock_workflow_finished_handle = MagicMock()

            mock_k8s_create_pod_manifest.return_value = job_manifest

            self.testee.dummy_db.append_config_map(
                self.workflow_id, K8S_CONFIGMAP_ID)

            # exercise
            self.testee.commit_workflow(
                workflow_id=self.workflow_id,
                workflow_resource=WORKFLOW_RESOURCE,
                workflow_finished_handle=mock_workflow_finished_handle)

            # verify
            mock_uuid4.assert_called_once()

            mock_k8s_create_pod_manifest.assert_called_once_with(
                job_uuid=self.job_id,
                job_config=WORKFLOW_RESOURCE,
                config_map_ref=[K8S_CONFIGMAP_ID],
                input_config_ref=None,
                input_resources=None,
                job_namespace=self.k8s_namespace,
                labels={"app": "gx4ki-demo",
                        "workflow-id": self.workflow_id,
                        "job-id": self.job_id})

            mock_k8s_create_pod.assert_called_once_with(
                manifest=job_manifest,
                namespace=self.k8s_namespace)

            mock_thread.assert_called_once()
            mock_thread_instance.start.assert_called_once()

            mock_event.assert_called_once()
            assert self.testee.dummy_db.get_job_monitor_event(
                self.workflow_id) == mock_event_instance

    def test_cleanup_monitor_thread(self):

        # setup
        job_id = "test"

        self.testee.dummy_db.data[WORKFLOW_ID] = K8sJobData(
            job_monitor_event=Event())

        # exercise
        self.testee._cleanup_monitor(WORKFLOW_ID)

        # verify
        self.assertTrue(
            self.testee.dummy_db.data[WORKFLOW_ID].job_monitor_event.is_set())

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

        with patch("middlelayer.backend.k8s_portforward", return_value=200) as mock_k8s_portforward:

            self.testee.store_result(
                workflow_id=workflow_id,
                workflow_store_info=workflow_store_info)

        mock_k8s_portforward.assert_called_once_with(
            data=workflow_store_info.json(),
            name=self.job_data.job_id,
            namespace=self.k8s_namespace)

    def test_getJobStateName(self):
        state = WorkflowJobState()

        job_state = "{\"phase\":\"FINISHED\",\"worker_state\":{\"event_type\":\"MODIFIED\",\"pod_phase\":\"Running\",\"pod_state_condition\":[\"{'last_probe_time': None,\\n 'last_transition_time': datetime.datetime(2023, 7, 26, 7, 38, 18, tzinfo=tzlocal()),\\n 'message': None,\\n 'reason': None,\\n 'status': 'True',\\n 'type': 'Initialized'}\",\"{'last_probe_time': None,\\n 'last_transition_time': datetime.datetime(2023, 7, 26, 7, 39, 21, tzinfo=tzlocal()),\\n 'message': 'containers with unready status: [worker]',\\n 'reason': 'ContainersNotReady',\\n 'status': 'False',\\n 'type': 'Ready'}\",\"{'last_probe_time': None,\\n 'last_transition_time': datetime.datetime(2023, 7, 26, 7, 39, 21, tzinfo=tzlocal()),\\n 'message': 'containers with unready status: [worker]',\\n 'reason': 'ContainersNotReady',\\n 'status': 'False',\\n 'type': 'ContainersReady'}\",\"{'last_probe_time': None,\\n 'last_transition_time': datetime.datetime(2023, 7, 26, 7, 38, 18, tzinfo=tzlocal()),\\n 'message': None,\\n 'reason': None,\\n 'status': 'True',\\n 'type': 'PodScheduled'}\"],\"container_statuses\":{\"data-side-car\":{\"state\":\"running\",\"details\":\"{'started_at': datetime.datetime(2023, 7, 26, 7, 38, 21, tzinfo=tzlocal())}\"},\"worker\":{\"state\":\"terminated\",\"details\":\"{'container_id': 'containerd://5232e7e460e1a0c4c91d4f66d9a677b81353021847eac800e267cab91e1c28a6',\\n 'exit_code': 0,\\n 'finished_at': datetime.datetime(2023, 7, 26, 7, 39, 20, tzinfo=tzlocal()),\\n 'message': None,\\n 'reason': 'Completed',\\n 'signal': None,\\n 'started_at': datetime.datetime(2023, 7, 26, 7, 38, 19, tzinfo=tzlocal())}\"}}}}"
        state = WorkflowJobState.parse_raw(job_state)

        import ast
        import json

        print(state.worker_state.pod_state_condition)
        test = [print(condition) for condition in state.worker_state.pod_state_condition]
        print(test)

        workflow_job_details = {"job_phase": state.phase.name,
                                "worker_details": state.worker_state.dict()}

        # print(workflow_job_details)
