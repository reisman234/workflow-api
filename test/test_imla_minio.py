import unittest
import io

from middlelayer.imla_minio import ImlaMinio
# from .context import middlelayer


def fun(x):
    return x+1


TEST_BUCKET = "test-bucket"

OBJECT_ID = "test-id"
OBJECT_NAME = "test-object"
OBJECT_CONTENT = b"test-content"


class TestImlaMinio(unittest.TestCase):

    def setUp(self) -> None:
        self.client = ImlaMinio(result_bucket=TEST_BUCKET)
        return super().setUp()

    def test_get_bucket_names(self):
        self.assertIn(TEST_BUCKET, self.client.get_bucket_names())

    def test_put_object(self):
        self.assertTrue(self.client.put_job_result(job_id=OBJECT_ID,
                                                   object_name=OBJECT_NAME,
                                                   content=io.BytesIO(
                                                       OBJECT_CONTENT),
                                                   content_length=len(
                                                       OBJECT_CONTENT)
                                                   ))
        self.assertIn(
            f"{OBJECT_ID}/{OBJECT_NAME}",
            self.client.list_job_result(OBJECT_ID)
        )

    def test_object_content(self):
        result = self.client.check_object_content(job_id=OBJECT_ID,
                                                  object_name=OBJECT_NAME,
                                                  content=OBJECT_CONTENT)
        self.assertTrue(result)


class MyTest(unittest.TestCase):
    def test(self):
        self.assertEqual(fun(3), 4)
