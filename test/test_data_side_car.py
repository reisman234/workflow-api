from unittest import TestCase

import requests

from middlelayer.models import WorkflowStoreInfo, MinioStoreInfo


class TestDataSideCar(TestCase):

    def test_store_workflow_result(self):

        # TODO
        # - manual deployment
        # - manual creation of test_file
        # - manual check in minio

        data_side_car_service = "http://192.168.49.2:31999/store/"

        store_info = WorkflowStoreInfo(
            minio=MinioStoreInfo(
                endpoint="141.79.45.112:9000",
                access_key="root",
                secret_key="changeme123",
                secure=False),
            destination_bucket="dummy-user-storage",
            destination_path="dummy/outputs",
            result_directory="/output",
            result_files=["test_file"])

        response = requests.post(
            url=data_side_car_service,
            json=store_info.dict(),
            timeout=30)

        self.assertTrue(200 <= response.status_code < 300,
                        f"status_code was {response.status_code}")
