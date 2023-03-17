import unittest
import io

from configparser import ConfigParser
from middlelayer.imla_minio import ImlaMinio
# from .context import middlelayer


def fun(x):
    return x+1


S_CONFIG = """
[minio]
endpoint = localhost:9000
access_key: root
secret_key: changeme123
secure: False
"""


class TestImlaMinio(unittest.TestCase):

    def setUp(self) -> None:
        config = ConfigParser()
        config.read_string(S_CONFIG)

        self.test_bucket = "test-bucket"
        self.object_id = "test-id"
        self.object_name = "test-object"
        self.object_content = b"test-content"

        self.testee = ImlaMinio(
            minio_config=config["minio"],
            result_bucket=self.test_bucket)

        if self.testee.bucket_exists(self.test_bucket):
            self.testee.remove_bucket(self.test_bucket, force=True)

        self.testee.create_bucket(self.test_bucket)
        self.test_put_object()

    def tearDown(self) -> None:
        pass
        # self.testee.remove_bucket(bucket_name=self.test_bucket, force=True)

    def test_get_bucket_names(self):
        self.assertIn(self.test_bucket, self.testee.get_bucket_names())

    def test_put_object(self):

        self.testee.put_job_result(job_id=self.object_id,
                                   object_name=self.object_name,
                                   content=io.BytesIO(
                                       self.object_content),
                                   content_length=len(
                                       self.object_content)
                                   )
        self.assertIn(
            f"{self.object_name}",
            self.testee.list_job_result(self.object_id)
        )

    def test_object_content(self):
        result = self.testee.check_object_content(job_id=self.object_id,
                                                  object_name=self.object_name,
                                                  content=self.object_content)
        self.assertTrue(result)

    def test_get_resource_data(self):
        data = self.testee.get_resource_data(
            bucket=self.test_bucket,
            resource=f"{self.object_id}/{self.object_name}")

        self.assertEqual(
            data,
            self.object_content.decode()
        )
