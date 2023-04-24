from minio import Minio
from minio.error import InvalidResponseError, ServerError, S3Error

from dags.dags_config import Config
from modules.log import log


@log
class MinioClient:
    def __init__(self):
        self.minio_client = Minio(**Config.MINIO_CONFIG)

    def create_bucket(self, bucket_name):
        try:
            self.minio_client.make_bucket(bucket_name=bucket_name)
        except ServerError as err:
            self.logger.error(err)
        except S3Error as err:
            self.logger.error(err)
        except InvalidResponseError as err:
            self.logger.error(err)

    def list_buckets(self):
        return self.minio_client.list_buckets()

    def bucket_exists(self, bucket_name):
        return self.minio_client.bucket_exists(bucket_name)

    def remove_bucket(self, bucket_name):
        return self.minio_client.remove_bucket(bucket_name)

    def list_objects(self, bucket_name):
        return self.minio_client.list_objects(bucket_name)

    def put_object(self, bucket_name, filename, filepath):
        try:
            self.minio_client.fput_object(bucket_name, filename, filepath)
        except InvalidResponseError as err:
            self.logger.error(err)

    def get_object(self, bucket_name, filename, filepath):
        response = None
        try:
            response = self.minio_client.fget_object(bucket_name, filename, filepath)
        except Exception as err:
            self.logger.error(err)

        return response

    def remove_object(self, bucket_name, filename):
        errors = self.minio_client.remove_object(bucket_name, filename)
        if errors:
            self.logger.error("error occurred when deleting object", errors)
