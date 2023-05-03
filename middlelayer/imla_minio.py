
from minio import Minio
from minio.deleteobjects import DeleteObject
# maybe use aws-s3 lib
# 1. verbindung nur öffnen wenn benötigt
# try&error replace return statements with yield
# s3 connection in its own process/thread (module storage communication)
# - close & open if needed
# - keep it open

from middlelayer.models import MinioStoreInfo
from datetime import timedelta
import sys

KB = 1024
MB = 1024*KB


class ImlaMinio():

    def __init__(self, minio_config, result_bucket):

        self.store_info = MinioStoreInfo(
            endpoint=minio_config.get('endpoint'),
            access_key=minio_config.get('access_key'),
            secret_key=minio_config.get('secret_key'),
            secure=minio_config.getboolean('secure')
        )
        self.client = Minio(
            endpoint=self.store_info.endpoint,
            access_key=self.store_info.access_key,
            secret_key=self.store_info.secret_key,
            secure=self.store_info.secure
        )
        self.result_bucket = result_bucket
        self._init_result_bucket()

    def _init_result_bucket(self):
        if not self.client.bucket_exists(self.result_bucket):
            self.client.make_bucket(self.result_bucket)

    def get_store_info(self) -> MinioStoreInfo:
        return self.store_info

    def get_bucket_names(self):
        buckets = self.client.list_buckets()
        return [b.name for b in buckets]

    def list_job_result(self, job_id):
        objects = self.client.list_objects(
            bucket_name=self.result_bucket,
            prefix=job_id,
            recursive=True)

        # remove the beginning directory name of each object
        object_list = [o.object_name.replace(
            f"{job_id}/", "") for o in objects]
        return object_list

    def get_object(self, job_id, object_name):
        return self.client.get_object(self.result_bucket,
                                      f"{job_id}/{object_name}",)

    def create_bucket(self, bucket_name):
        self.client.make_bucket(bucket_name=bucket_name)

    def remove_bucket(self, bucket_name, force=False):
        if force:
            delete_object_list = [DeleteObject(x.object_name) for x in self.client.list_objects(bucket_name=bucket_name,
                                                                                                recursive=True)]
            for err in self.client.remove_objects(bucket_name=bucket_name,
                                                  delete_object_list=delete_object_list):
                print(err)

        self.client.remove_bucket(bucket_name=bucket_name)

    def bucket_exists(self, bucket_name):
        return self.client.bucket_exists(bucket_name=bucket_name)

    def put_job_result(self, job_id, object_name, content, content_length):
        '''
        for testing purpose, put a file into a specific bucket/directory
        '''
        response = self.client.put_object(bucket_name=self.result_bucket,
                                          object_name=f"{job_id}/{object_name}",
                                          data=content,
                                          length=content_length)
        print(response)
        return True

    def check_object_content(self, job_id, object_name, content):
        result = False
        try:
            response = self.client.get_object(
                self.result_bucket, f"{job_id}/{object_name}")
            result = content == response.read()
        finally:
            response.close()
            response.release_conn()

        return result

    def get_resource_data(self, bucket, resource):
        try:
            response = self.client.get_object(bucket_name=bucket,
                                              object_name=resource)

            return response.data.decode(
                encoding="utf-8"
            )
        finally:
            response.close()
            response.release_conn()

    def put_file(self,
                 bucket,
                 resource,
                 file):

        self.client.put_object(
            bucket_name=bucket,
            object_name=resource,
            data=file.file,
            length=-1,
            part_size=64*MB)

    def get_file(self,
                 bucket,
                 resource):

        response = self.client.get_object(
            bucket_name=bucket,
            object_name=resource)

        return response

    def get_objects_list(self, bucket, prefix=None):
        objects = self.client.list_objects(bucket_name=bucket,
                                           prefix=prefix,
                                           recursive=True)
        return [o.object_name for o in objects]

    def get_download_url(self, bucket, resource):
        return self.client.presigned_get_object(bucket_name=bucket,
                                                object_name=resource,
                                                expires=timedelta(hours=1, minutes=30))

    def get_upload_url(self, bucket_name, response_name):
        return self.client.presigned_put_object(bucket_name=bucket_name,
                                                object_name=response_name,
                                                expires=timedelta(hours=1, minutes=30))
