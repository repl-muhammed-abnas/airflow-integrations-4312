# pylint: disable=bare-except
from io import BytesIO
from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
import boto3
import pandas as pd


class DownloadCsvOperator(BaseOperator):
    template_fields = ["bucket_name", "file_path", "file_name"]

    def __init__(self,
                 *,
                 bucket_name: str,
                 file_path: str,
                 file_name: str,
                 expires_in_seconds: int,
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.expires_in_seconds = expires_in_seconds
        self.file_path = file_path
        self.file_name = file_name

    def execute(self, context):
        s3_key = f'artifacts/{self.file_path}/{self.file_name}'
        s3_region = Variable.get("CIE_AWS_REGION")
        access_id = Variable.get("SECRET_CIE_AWS_ID")
        secret_key = Variable.get("SECRET_CIE_AWS_KEY")
        bucket = self.bucket_name
        artifact_name = f'artifact/{self.file_path}/{self.file_name}'.replace(
            "/", ":")
        self.log.info(
            f"get data from file for artifact {artifact_name} with s3-key {s3_key}")
        try:
            s3_client = boto3.client(service_name='s3',
                                     region_name=s3_region,
                                     aws_access_key_id=access_id,
                                     aws_secret_access_key=secret_key)

            s3_client.head_object(Bucket=bucket, Key=s3_key)
            response = s3_client.get_object(
                Bucket=bucket, Key=s3_key)
            data = BytesIO(response['Body'].read())
            df = pd.read_csv(data)
            return df.to_json()
        except:
            return {}
