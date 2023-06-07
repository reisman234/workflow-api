from unittest import TestCase
from unittest.mock import patch, MagicMock, Mock
from io import BytesIO

from fastapi.testclient import TestClient


from middlelayer.models import ServiceDescription, ServiceResouce, WorkflowResource
import middlelayer.service_api as testee_mod
from middlelayer.service_api import service_api, ServiceApi, SERVICE_DESCRIPTIONS, SERVICE_ID_CARLA
from middlelayer.service_api import ImlaMinio, K8sWorkflowBackend


class TestServiceApi(TestCase):

    def setUp(self) -> None:
        # TODO does not work?
        # self.client = TestClient(service_api)
        self.headers = {"access-token": "pass"}
        testee_mod.SERVICES.clear()
        testee_mod.SERVICES["test_service"] = {"test_id": "service_info"}
        testee_mod.WORKFLOW_API_ACCESS_TOKEN = "pass"

        self.storage_bucket = "test_bucket"

        self.test_service_id = "test_id"
        self.test_service = ServiceDescription(
            service_id=self.test_service_id,
            inputs=[ServiceResouce(
                resource_name="test_res_in",
                type=1,
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

        testee_mod.SERVICE_DESCRIPTIONS.clear()
        testee_mod.SERVICE_DESCRIPTIONS["test_id"] = self.test_service

    def test_get_service(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            response = test_client.get("/services/")
            self.assertTrue(response.is_error)
            self.assertTrue(response.status_code ==
                            testee_mod.HTTP_403_FORBIDDEN)

            response = test_client.get("/services/", headers=self.headers)
            self.assertFalse(response.is_error)
            self.assertEqual(response.json(), testee_mod.SERVICES)

    def test_get_service_desciption(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            response = test_client.get(
                f"/services/{self.test_service_id}/info/")
            self.assertTrue(response.is_error)
            self.assertTrue(response.status_code ==
                            testee_mod.HTTP_403_FORBIDDEN)

            response = test_client.get(
                f"/services/{self.test_service_id}/info/", headers=self.headers)

            self.assertFalse(response.is_error)
            self.assertEqual(
                ServiceDescription(**response.json()),
                self.test_service)

            response = test_client.get(
                f"/services/fake/info/", headers=self.headers)

            self.assertTrue(response.status_code ==
                            testee_mod.HTTP_400_BAD_REQUEST)
            self.assertTrue(response.is_client_error)

    def test_get_service_input_info_missing_auth(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            response = test_client.put(
                f"/services/{self.test_service_id}/input/test_res_in")
            self.assertTrue(response.is_client_error)
            self.assertTrue(response.status_code ==
                            testee_mod.HTTP_403_FORBIDDEN)

    def test_get_service_input_info_invalid_resource(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            response = test_client.put(
                f"/services/{self.test_service_id}/input/invalid",
                headers=self.headers,
                follow_redirects=False,
                files={"input_file": b"data"})

            self.assertTrue(response.is_client_error)
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_400_BAD_REQUEST)

    def test_get_service_input_info_valid_resource(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            mock_storage_instance: MagicMock = mock_storage_backend.return_value

            response = test_client.put(
                f"/services/{self.test_service_id}/input/test_res_in",
                headers=self.headers,
                follow_redirects=False,
                files={"input_file": b"data"})

            self.assertTrue(response.is_success)
            mock_storage_instance.put_file.assert_called_once()

    def test_get_service_output_info_missing_auth(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            response = test_client.get(
                f"/services/{self.test_service_id}/output/test_res_in")
            self.assertTrue(response.is_client_error)
            self.assertTrue(response.status_code ==
                            testee_mod.HTTP_403_FORBIDDEN)

    def test_get_service_output_info_invalid_resource(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            response = test_client.get(
                f"/services/{self.test_service_id}/output/invalid",
                headers=self.headers)
            self.assertTrue(response.is_client_error)
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_400_BAD_REQUEST)
            self.assertEqual(response.json()["detail"],
                             "no valid resource provided")

    def test_get_service_output_info_valid_resource_not_exists(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            mock_storage_instance = mock_storage_backend.return_value
            mock_storage_instance.get_objects_list.return_value = []

            response = test_client.get(
                f"/services/{self.test_service_id}/output/test_res_out",
                headers=self.headers)
            self.assertTrue(response.is_client_error)
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_404_NOT_FOUND)
            self.assertEqual(
                response.json()["detail"], "requested resource not exists")

    def test_get_service_output_info_valid_resource(self):

        fake_content = b"test"
        fake_file = BytesIO()
        fake_file.write(fake_content)
        fake_file.seek(0)

        mock_response = MagicMock()
        mock_response.read.return_value = fake_file.read()
        mock_response.header = {"Content-Type": "application/plain", "Content-Length": "4"}

        test_resource_storage_name = f"{self.test_service_id}/outputs/test_res_out"

        # mock_storage_backend.get_upload_url.return_value = "fake_upload_url"

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            mock_storage_instance = mock_storage_backend.return_value
            mock_storage_instance.get_objects_list.return_value = [
                test_resource_storage_name]
            mock_storage_instance.get_file.return_value = mock_response

            response = test_client.get(
                f"/services/{self.test_service_id}/output/test_res_out",
                headers=self.headers)

            self.assertTrue(response.is_success)
            self.assertEqual(response.content, fake_content)

    def test_post_start_service_workflow_with_insufficient_resource(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            mock_storage_instance = mock_storage_backend.return_value
            mock_storage_instance.get_objects_list.return_value = []

            response = test_client.post(
                f"/services/{self.test_service_id}/workflow/execute",
                headers=self.headers)

            self.assertTrue(response.is_client_error)
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_400_BAD_REQUEST)
            self.assertEqual(response.json()["detail"],
                             "service input not fulfilled")

            mock_storage_instance.get_objects_list.assert_called_once()

    def test_post_start_service_workflow_with_resource(self):
        test_resource_storage_name = f"{self.test_service_id}/inputs/test_res_in"

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                patch("middlelayer.service_api.uuid4",
                      return_value="fake_workflow_id"),\
                patch("middlelayer.service_api.Thread") as mock_thread,\
                TestClient(service_api) as test_client:

            mock_storage_instance = mock_storage_backend.return_value
            mock_storage_instance.get_objects_list.return_value = [
                test_resource_storage_name]

            mock_workflow_instance = mock_workflow_backend.return_value
            mock_workflow_instance.handle_input.return_value = None

            mock_thread_instance = mock_thread.return_value

            response = test_client.post(
                f"/services/{self.test_service_id}/workflow/execute",
                headers=self.headers)

            self.assertFalse(response.is_client_error)
            self.assertEqual(response.status_code,
                             200)
            self.assertEqual(response.json()["workflow_id"],
                             "fake_workflow_id")

            mock_thread.assert_called_once()
            mock_thread_instance.start.assert_called_once()

    def test_get_service_workflow_status_missing_auth(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            service_id = "fake_service_id"
            workflow_id = "fake_workflow_id"
            url = f"/services/{service_id}/workflow/status/{workflow_id}"

            response = test_client.get(url)

            self.assertTrue(response.is_client_error)
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_403_FORBIDDEN)

    def test_get_service_workflow_status_invalid_service_id(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            service_id = "fake_service_id"
            workflow_id = "fake_workflow_id"

            url = f"/services/{service_id}/workflow/status/{workflow_id}"

            response = test_client.get(url,
                                       headers=self.headers)

            self.assertTrue(response.is_error)
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_400_BAD_REQUEST)
            self.assertEqual(response.json()["detail"],
                             "no valid service_id")

            mock_workflow_backend.assert_called_once()
            mock_storage_backend.assert_called_once()

    def test_get_service_workflow_status_invalid_workflow_id(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                patch.object(ServiceApi, "workflow_exists", return_value=False),\
                TestClient(service_api) as test_client:

            service_id = self.test_service_id
            workflow_id = "fake_workflow_id"

            url = f"/services/{service_id}/workflow/status/{workflow_id}"

            response = test_client.get(url,
                                       headers=self.headers)

            self.assertTrue(response.is_error)
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_400_BAD_REQUEST)
            self.assertEqual(response.json()["detail"],
                             "invalid workflow_id")

            mock_workflow_backend.assert_called_once()
            mock_storage_backend.assert_called_once()

    def test_get_service_workflow_status(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                patch.object(ServiceApi, "workflow_exists", return_value=True),\
                TestClient(service_api) as test_client:

            service_id = self.test_service_id
            workflow_id = "fake_workflow_id"
            workflow_status = "fake_workflow_status"
            test_workflow_status = {"service_id": service_id,
                                    "workflow_id": workflow_id,
                                    "workflow_status": workflow_status}

            mock_workflow_instance = mock_workflow_backend.return_value
            mock_workflow_instance.get_status.return_value = workflow_status

            url = f"/services/{service_id}/workflow/status/{workflow_id}"

            response = test_client.get(url,
                                       headers=self.headers)

            self.assertFalse(response.is_error)
            self.assertEqual(response.status_code,
                             200)
            self.assertEqual(response.json(),
                             test_workflow_status)

            mock_workflow_backend.assert_called_once()
            mock_storage_backend.assert_called_once()

            mock_workflow_instance.get_status.assert_called_once_with(
                workflow_id=workflow_id,
                verbose_level=0)

    def test_post_stop_service_workflow_missing_auth(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            service_id = "fake_service_id"
            workflow_id = "fake_workflow_id"
            url = f"/services/{service_id}/workflow/stop/{workflow_id}"

            response = test_client.post(url)

            self.assertTrue(response.is_client_error)
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_403_FORBIDDEN)

    def test_post_stop_service_workflow_invalid_service_id(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                TestClient(service_api) as test_client:

            service_id = "fake_service_id"
            workflow_id = "fake_workflow_id"
            url = f"/services/{service_id}/workflow/stop/{workflow_id}"

            response = test_client.post(url,
                                        headers=self.headers)

            self.assertTrue(response.is_error)
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_400_BAD_REQUEST)
            self.assertEqual(response.json()["detail"],
                             "no valid service_id")

    def test_post_stop_service_workflow_invalid_workflow_id(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                patch.object(ServiceApi, "workflow_exists", return_value=False),\
                TestClient(service_api) as test_client:

            service_id = self.test_service_id
            workflow_id = "fake_workflow_id"
            url = f"/services/{service_id}/workflow/stop/{workflow_id}"

            response = test_client.post(url,
                                        headers=self.headers)

            self.assertTrue(response.is_error)
            self.assertEqual(response.status_code,
                             testee_mod.HTTP_400_BAD_REQUEST)
            self.assertEqual(response.json()["detail"],
                             "invalid workflow_id")

    def test_post_stop_service_workflow(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend,\
                patch.object(ServiceApi, "workflow_exists", return_value=True),\
                TestClient(service_api) as test_client:

            mock_workflow_instance = mock_workflow_backend.return_value

            service_id = self.test_service_id
            workflow_id = "fake_workflow_id"
            url = f"/services/{service_id}/workflow/stop/{workflow_id}"

            response = test_client.post(url,
                                        headers=self.headers)

            self.assertFalse(response.is_error)
            self.assertEqual(response.status_code,
                             200)
            self.assertEqual(response.json(),
                             {})

            mock_workflow_backend.assert_called_once()
            mock_storage_backend.assert_called_once()

            mock_workflow_instance.cleanup.assert_called_once_with(
                workflow_id=workflow_id)

    def test_service_api_commit_workflow(self):

        with patch("middlelayer.service_api.K8sWorkflowBackend") as mock_workflow_backend,\
                patch("middlelayer.service_api.ImlaMinio") as mock_storage_backend:

            mock_workflow_instance = mock_workflow_backend.return_value

            mock_storage_instance = mock_storage_backend.return_value

            service_id = self.test_service_id
            workflow_id = "fake_workflow_id"
            testee = ServiceApi()

            testee.commit_task(service_id=service_id,
                               workflow_id=workflow_id)

            for input in self.test_service.inputs:
                mock_workflow_instance.handle_input.assert_called()

            mock_workflow_instance.commit_workflow.assert_called_once()
