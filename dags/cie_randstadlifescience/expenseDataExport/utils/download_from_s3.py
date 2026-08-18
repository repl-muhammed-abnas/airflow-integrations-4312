# pylint: disable=bare-except,ungrouped-imports
from airflow.models.baseoperator import BaseOperator
import boto3
from airflow.models import Variable

class DownloadAllFilesOperator(BaseOperator):
    template_fields = ["file_path"]

    def __init__(self,
                 *,
                 file_path: str,
                 bucket_name: str,
                 expires_in_seconds: int,
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)
        self.expires_in_seconds = expires_in_seconds
        self.file_path = file_path
        self.bucket_name = bucket_name

    def execute(self, context):
        bucket = self.bucket_name
        s3_region = Variable.get("CIE_AWS_REGION")
        access_id = Variable.get("SECRET_CIE_AWS_ID")
        secret_key = Variable.get("SECRET_CIE_AWS_KEY")
        s3_client = boto3.client(service_name='s3',
                                region_name=s3_region,
                                aws_access_key_id=access_id,
                                aws_secret_access_key=secret_key)
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix = self.file_path)
        d = []
        if "Contents" not in response:
            return []
        for content in response['Contents']:
            f_path = content['Key']
            if f_path == "" or not f_path:
                continue
            s3_client.head_object(Bucket=bucket, Key=f_path)
            response_data = s3_client.get_object(
                Bucket=bucket, Key=f_path)
            data = response_data['Body'].read().decode('utf-8')
            d.append({"data":data,"s3_key": f_path})
        return d

class DownloadFileOperator(BaseOperator):
    template_fields = ["file_path", "file_name"]

    def __init__(self,
                 *,
                 file_path: str,
                 file_name: str,
                 bucket_name: str,
                 expires_in_seconds: int,
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)
        self.expires_in_seconds = expires_in_seconds
        self.file_path = file_path
        self.file_name = file_name
        self.bucket_name = bucket_name

    def execute(self, context):
        s3_key = f'{self.file_path}/{self.file_name}'
        bucket = self.bucket_name
        artifact_name = f'{self.file_path}/{self.file_name}'.replace(
            "/", ":")
        self.log.info(
            f"get data from file for artifact {artifact_name} with s3-key {s3_key}")
        s3_region = Variable.get("CIE_AWS_REGION")
        access_id = Variable.get("SECRET_CIE_AWS_ID")
        secret_key = Variable.get("SECRET_CIE_AWS_KEY")
        s3_client = boto3.client(service_name='s3',
                                region_name=s3_region,
                                aws_access_key_id=access_id,
                                aws_secret_access_key=secret_key)
        # try:
        s3_client.head_object(Bucket=bucket, Key=s3_key)
        response = s3_client.get_object(
            Bucket=bucket, Key=s3_key)
        data = response['Body'].read().decode('utf-8')
        if len(data) > 0:
            return data
        return ""
        # except Exception as e:
        #     return {"error": str(e)}
