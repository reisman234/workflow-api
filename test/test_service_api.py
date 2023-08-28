from unittest import TestCase
from unittest.mock import patch, MagicMock, Mock
from io import BytesIO

from fastapi.testclient import TestClient


from middlelayer.models import ServiceDescription, InputServiceResource, ServiceResouce, WorkflowResource
import middlelayer.service_api as testee_mod
from middlelayer.service_api import service_api, ServiceApi

from middlelayer.backend import WorkflowJobState


class TestServiceApi(TestCase):

    def setUp(self) -> None:
        # TODO does not work?
        # self.client = TestClient(service_api)
        self.headers = {"access-token": "pass"}
        # testee_mod.SERVICES.clear()
        # testee_mod.SERVICES["test_service"] = {"test_id": "service_info"}
        testee_mod.WORKFLOW_API_ACCESS_TOKEN = "pass"

        self.storage_bucket = "test_bucket"

        self.test_service_id = "test_id"
        self.test_service = ServiceDescription(
            service_id=self.test_service_id,
            inputs=[InputServiceResource(
                resource_name="test_res_in",
                type=1,
                mount_path="/test/path",
                description="test_description"),],
            outputs=[ServiceResouce(
                resource_name="test_res_out",
                type=2,
                description="test_description")],
            workflow_resource=WorkflowResource(
                worker_image="test",
                worker_image_output_directory="test",
                gpu=False)
        )

        def side_effect(service_id):
            if service_id == self.test_service_id:
                return self.test_service
            return None

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                patch("middlelayer.service_api.StaticAssetLoader") as mock_asset_loader,\
                TestClient(service_api) as client:

            self.mock_asset_loader_instance = mock_asset_loader.return_value
            self.mock_asset_loader_instance.get_assets.return_value = {self.test_service_id: self.test_service}
            self.mock_asset_loader_instance.get_assets_description.side_effect = side_effect

            self.mock_workflow_backend = mock_workflow_backend
            self.mock_workflow_storage = mock_storage_backend
            self.testee = client
        # testee_mod.SERVICE_DESCRIPTIONS.clear()
        # testee_mod.SERVICE_DESCRIPTIONS["test_id"] = self.test_service

    def test_get_service(self):

        response = self.testee.get("/services/")
        self.assertTrue(response.is_error)
        self.assertTrue(response.status_code ==
                        testee_mod.HTTP_403_FORBIDDEN)

        response = self.testee.get("/services/", headers=self.headers)
        self.assertFalse(response.is_error)
        self.assertIsNotNone(response.json())
        self.assertEqual(response.json(),
                         {self.test_service_id: self.test_service.model_dump()})

    def test_get_service_desciption(self):

        response = self.testee.get(
            f"/services/{self.test_service_id}/info/")
        self.assertTrue(response.is_error)
        self.assertTrue(response.status_code ==
                        testee_mod.HTTP_403_FORBIDDEN)

        response = self.testee.get(
            f"/services/{self.test_service_id}/info/", headers=self.headers)

        self.assertFalse(response.is_error)
        self.assertEqual(
            ServiceDescription(**response.json()),
            self.test_service)

        response = self.testee.get(
            "/services/fake/info/", headers=self.headers)

        self.assertTrue(response.status_code ==
                        testee_mod.HTTP_400_BAD_REQUEST)
        self.assertTrue(response.is_client_error)

    def test_get_service_input_info_missing_auth(self):

        response = self.testee.put(
            f"/services/{self.test_service_id}/input/test_res_in")
        self.assertTrue(response.is_client_error)
        self.assertTrue(response.status_code ==
                        testee_mod.HTTP_403_FORBIDDEN)

    def test_get_service_input_info_invalid_resource(self):

        # with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
        #         patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
        #         TestClient(service_api) as test_client:

        response = self.testee.put(
            f"/services/{self.test_service_id}/input/invalid",
            headers=self.headers,
            follow_redirects=False,
            files={"input_file": b"data"})

        self.assertTrue(response.is_client_error)
        self.assertEqual(response.status_code,
                         testee_mod.HTTP_400_BAD_REQUEST)

    def test_put_valid_resource(self):

        mock_storage_instance: MagicMock = self.mock_workflow_storage.return_value

        response = self.testee.put(
            f"/services/{self.test_service_id}/input/test_res_in",
            headers=self.headers,
            follow_redirects=False,
            files={"input_file": b"data"})

        self.assertEqual(response.status_code, testee_mod.HTTP_200_OK, f"status_code was {response.status_code}")
        mock_storage_instance.put_file.assert_called_once()

    def test_get_output_resource_missing_auth(self):

        response = self.testee.get(
            f"/services/{self.test_service_id}/output",
            params={"resource": "test_res_out"})
        self.assertTrue(response.is_client_error)
        self.assertEqual(response.status_code,
                         testee_mod.HTTP_403_FORBIDDEN)

    def test_get_output_resource_invalid_resource(self):

        response = self.testee.get(
            f"/services/{self.test_service_id}/output",
            params={"resource": "invalid"},
            headers=self.headers)
        self.assertTrue(response.is_client_error)
        self.assertEqual(response.status_code,
                         testee_mod.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.json()["detail"],
                         "no valid resource provided")

    def test_get_output_resource_valid_resource_not_exists(self):

        mock_storage_instance = self.mock_workflow_storage.return_value
        mock_storage_instance.get_objects_list.return_value = []

        response = self.testee.get(
            f"/services/{self.test_service_id}/output",
            params={"resource": "test_res_out"},
            headers=self.headers)
        self.assertTrue(response.is_client_error)
        self.assertEqual(response.status_code,
                         testee_mod.HTTP_404_NOT_FOUND)
        self.assertEqual(
            response.json()["detail"], "requested resource not exists")

    def test_get_output_resource_valid_resource(self):

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {'Content-Type': 'text/plain', "Content-Length": "4", 'Custom-Header': 'Mocked'}
        mock_response.stream = lambda: (x for x in ["test"])

        test_resource_storage_name = "test_res_out"

        mock_storage_instance = self.mock_workflow_storage.return_value
        mock_storage_instance.get_objects_list.return_value = [
            test_resource_storage_name]
        mock_storage_instance.get_file.return_value = mock_response

        response = self.testee.get(
            f"/services/{self.test_service_id}/output",
            params={"resource": "test_res_out"},
            headers=self.headers)

        self.assertTrue(response.is_success)
        self.assertEqual(response.content, b"test")

    def test_post_start_service_workflow_with_insufficient_resource(self):

        mock_storage_instance = self.mock_workflow_storage.return_value
        mock_storage_instance.get_objects_list.return_value = []

        response = self.testee.post(
            f"/services/{self.test_service_id}/workflow/execute",
            headers=self.headers)

        self.assertTrue(response.is_client_error)
        self.assertEqual(response.status_code,
                         testee_mod.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.json()["detail"],
                         "service input not fulfilled")

        mock_storage_instance.get_objects_list.assert_called_once()

    def test_post_start_service_workflow_with_resource(self):
        test_resource_storage_name = "test_res_in"

        with patch("middlelayer.service_api.uuid4",
                   return_value="fake_workflow_id"),\
                patch("middlelayer.service_api.Thread") as mock_thread:

            mock_storage_instance = self.mock_workflow_storage.return_value
            mock_storage_instance.get_objects_list.return_value = [
                test_resource_storage_name]

            mock_workflow_instance = self.mock_workflow_backend.return_value
            mock_workflow_instance.handle_input.return_value = None

            mock_thread_instance = mock_thread.return_value

            response = self.testee.post(
                f"/services/{self.test_service_id}/workflow/execute",
                headers=self.headers)

            self.assertTrue(response.is_success, "request not succeeded")
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_200_OK)
            self.assertEqual(response.json()["workflow_id"],
                             "fake_workflow_id")

            mock_thread.assert_called_once()
            mock_thread_instance.start.assert_called_once()

    def test_get_service_workflow_status_missing_auth(self):

        service_id = "fake_service_id"
        workflow_id = "fake_workflow_id"
        url = f"/services/{service_id}/workflow/status/{workflow_id}"

        response = self.testee.get(url)

        self.assertTrue(response.is_client_error)
        self.assertEqual(response.status_code,
                         testee_mod.HTTP_403_FORBIDDEN)

    def test_get_service_workflow_status_invalid_service_id(self):

        service_id = "fake_service_id"
        workflow_id = "fake_workflow_id"

        url = f"/services/{service_id}/workflow/status/{workflow_id}"

        response = self.testee.get(url,
                                   headers=self.headers)

        self.assertTrue(response.is_error)
        self.assertEqual(response.status_code,
                         testee_mod.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.json()["detail"],
                         "no valid service_id")

        self.mock_workflow_backend.assert_called_once()
        self.mock_workflow_storage.assert_called_once()

    def test_get_service_workflow_status_invalid_workflow_id(self):

        with patch.object(ServiceApi, "workflow_exists", return_value=False):

            service_id = self.test_service_id
            workflow_id = "fake_workflow_id"

            url = f"/services/{service_id}/workflow/status/{workflow_id}"

            response = self.testee.get(url,
                                       headers=self.headers)

            self.assertTrue(response.is_error)
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_400_BAD_REQUEST)
            self.assertEqual(response.json()["detail"],
                             "invalid workflow_id")

            self.mock_workflow_backend.assert_called_once()
            self.mock_workflow_storage.assert_called_once()

    def test_get_service_workflow_status(self):

        with patch.object(ServiceApi, "workflow_exists", return_value=True):

            service_id = self.test_service_id
            workflow_id = "fake_workflow_id"
            workflow_status = MagicMock()
            workflow_status.model_dump.return_value = {"details": "fake_workflow_status"}
            test_workflow_status = {"service_id": service_id,
                                    "workflow_id": workflow_id,
                                    "workflow_status": {"details": "fake_workflow_status"}}

            mock_workflow_instance = self.mock_workflow_backend.return_value
            mock_workflow_instance.get_status.return_value = workflow_status

            url = f"/services/{service_id}/workflow/status/{workflow_id}"

            response = self.testee.get(url,
                                       headers=self.headers)

            self.assertTrue(response.is_success)
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_200_OK)
            self.assertEqual(response.json(),
                             test_workflow_status)

            self.mock_workflow_backend.assert_called_once()
            self.mock_workflow_storage.assert_called_once()

            mock_workflow_instance.get_status.assert_called_once_with(
                workflow_id=workflow_id,
                verbose_level=0)

    def test_post_stop_service_workflow_missing_auth(self):

        service_id = "fake_service_id"
        workflow_id = "fake_workflow_id"
        url = f"/services/{service_id}/workflow/stop/{workflow_id}"

        response = self.testee.post(url)

        self.assertTrue(response.is_client_error)
        self.assertEqual(response.status_code,
                         testee_mod.HTTP_403_FORBIDDEN)

    def test_post_stop_service_workflow_invalid_service_id(self):

        service_id = "fake_service_id"
        workflow_id = "fake_workflow_id"
        url = f"/services/{service_id}/workflow/stop/{workflow_id}"

        response = self.testee.post(url,
                                    headers=self.headers)

        self.assertTrue(response.is_error)
        self.assertEqual(response.status_code,
                         testee_mod.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.json()["detail"],
                         "no valid service_id")

    def test_post_stop_service_workflow_invalid_workflow_id(self):

        with patch.object(ServiceApi, "workflow_exists", return_value=False):

            service_id = self.test_service_id
            workflow_id = "fake_workflow_id"
            url = f"/services/{service_id}/workflow/stop/{workflow_id}"

            response = self.testee.post(url,
                                        headers=self.headers)

            self.assertTrue(response.is_error)
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_400_BAD_REQUEST)
            self.assertEqual(response.json()["detail"],
                             "invalid workflow_id")

    def test_post_stop_service_workflow(self):

        with patch.object(ServiceApi, "workflow_exists", return_value=True):

            mock_workflow_instance = self.mock_workflow_backend.return_value

            service_id = self.test_service_id
            workflow_id = "fake_workflow_id"
            url = f"/services/{service_id}/workflow/stop/{workflow_id}"

            response = self.testee.post(url,
                                        headers=self.headers)

            self.assertFalse(response.is_error)
            self.assertEqual(response.status_code,
                             200)
            self.assertEqual(response.json(),
                             {})

            self.mock_workflow_backend.assert_called_once()
            self.mock_workflow_storage.assert_called_once()

            mock_workflow_instance.cleanup.assert_called_once_with(
                workflow_id=workflow_id)

    def test_service_api_commit_workflow(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.StaticAssetLoader") as mock_asset_loader,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend:

            self.mock_asset_loader_instance = mock_asset_loader.return_value
            self.mock_asset_loader_instance.get_assets.return_value = {self.test_service_id: self.test_service}
            self.mock_asset_loader_instance.get_assets_description.return_value = self.test_service

            mock_workflow_instance = mock_workflow_backend.return_value

            mock_storage_instance = self.mock_workflow_storage.return_value

            service_id = self.test_service_id
            workflow_id = "fake_workflow_id"
            testee = ServiceApi()

            testee.commit_task(service_id=service_id,
                               workflow_id=workflow_id)

            mock_workflow_instance.handle_input.assert_called_once()

            mock_workflow_instance.commit_workflow.assert_called_once()

    def test_get_workflow_status(self):
        """
        test to get the status of the workflow
        - workflow_backend is mocked and get_status will return a fake WorkflowJobState
        """

        with patch("middlelayer.service_api.ServiceApi.workflow_exists", return_value=True):

            mock_workflow_instance = self.mock_workflow_backend.return_value

            fake_job_state = WorkflowJobState.parse_raw("{\"phase\":\"FINISHED\",\"worker_state\":{\"event_type\":\"MODIFIED\",\"pod_phase\":\"Running\",\"pod_state_condition\":[\"{'last_probe_time': None,\\n 'last_transition_time': datetime.datetime(2023, 7, 26, 7, 38, 18, tzinfo=tzlocal()),\\n 'message': None,\\n 'reason': None,\\n 'status': 'True',\\n 'type': 'Initialized'}\",\"{'last_probe_time': None,\\n 'last_transition_time': datetime.datetime(2023, 7, 26, 7, 39, 21, tzinfo=tzlocal()),\\n 'message': 'containers with unready status: [worker]',\\n 'reason': 'ContainersNotReady',\\n 'status': 'False',\\n 'type': 'Ready'}\",\"{'last_probe_time': None,\\n 'last_transition_time': datetime.datetime(2023, 7, 26, 7, 39, 21, tzinfo=tzlocal()),\\n 'message': 'containers with unready status: [worker]',\\n 'reason': 'ContainersNotReady',\\n 'status': 'False',\\n 'type': 'ContainersReady'}\",\"{'last_probe_time': None,\\n 'last_transition_time': datetime.datetime(2023, 7, 26, 7, 38, 18, tzinfo=tzlocal()),\\n 'message': None,\\n 'reason': None,\\n 'status': 'True',\\n 'type': 'PodScheduled'}\"],\"container_statuses\":{\"data-side-car\":{\"state\":\"running\",\"details\":\"{'started_at': datetime.datetime(2023, 7, 26, 7, 38, 21, tzinfo=tzlocal())}\"},\"worker\":{\"state\":\"terminated\",\"details\":\"{'container_id': 'containerd://5232e7e460e1a0c4c91d4f66d9a677b81353021847eac800e267cab91e1c28a6',\\n 'exit_code': 0,\\n 'finished_at': datetime.datetime(2023, 7, 26, 7, 39, 20, tzinfo=tzlocal()),\\n 'message': None,\\n 'reason': 'Completed',\\n 'signal': None,\\n 'started_at': datetime.datetime(2023, 7, 26, 7, 38, 19, tzinfo=tzlocal())}\"}}}}")

            mock_workflow_instance.get_status.return_value = fake_job_state
            mock_storage_instance = self.mock_workflow_storage.return_value

            service_id = self.test_service_id
            workflow_id = "fake_workflow_id"
            url = f"/services/{service_id}/workflow/status/{workflow_id}"

            response = self.testee.get(url=url,
                                       headers=self.headers)

            self.assertTrue(response.is_success)
            self.assertEqual(response.json()["workflow_status"]["phase"], "FINISHED")
            # for input in self.test_service.inputs:
            #     mock_workflow_instance.handle_input.assert_called()

            mock_workflow_instance.get_status.assert_called_once()
