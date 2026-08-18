# pylint: disable=bare-except,ungrouped-imports
import json
from airflow.models.baseoperator import BaseOperator
import boto3
from airflow.models import Variable

class DeleteFilesOperator(BaseOperator):
    template_fields = ["file_path", "s3_keys"]

    def __init__(self,
                 *,
                 file_path: str,
                 bucket_name: str,
                 s3_keys: str,
                 expires_in_seconds: int,
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)
        self.expires_in_seconds = expires_in_seconds
        self.file_path = file_path
        self.bucket_name = bucket_name
        self.s3_keys = s3_keys

    def execute(self, context):
        s3_keys_list = json.loads(self.s3_keys)
        s3_region = Variable.get("CIE_AWS_REGION")
        access_id = Variable.get("SECRET_CIE_AWS_ID")
        secret_key = Variable.get("SECRET_CIE_AWS_KEY")
        s3_client = boto3.client(service_name='s3',
                                         region_name=s3_region,
                                         aws_access_key_id=access_id,
                                         aws_secret_access_key=secret_key)
        response = s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix = self.file_path)
        deleted_list = []
        for content in response['Contents']:
            f_path = content['Key']
            if f_path == "" or not f_path or f_path not in s3_keys_list:
                continue
            self.log.info(
            f"Deleting file with s3-key {f_path}")
            s3_client.delete_object(Bucket=self.bucket_name, Key=f_path)
            deleted_list.append(f_path)
        if len(deleted_list) > 0:
            return {"deleted_files": ", ".join(deleted_list)}
        return {"delete_files": "No files to delete"}
