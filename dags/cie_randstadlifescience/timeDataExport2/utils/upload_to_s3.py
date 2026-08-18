# pylint: disable=bare-except
import json
import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
import boto3


class UploadCsvOperator(BaseOperator):
    template_fields = ["source", "bucket_name", "file_path",
                       "file_name", "delimiter", "encoding"]

    def __init__(self, *, source, bucket_name: str,
                 file_path: str, file_name: str, delimiter: str = ',', encoding: str = 'utf-8', **kwargs) -> None:
        super().__init__(**kwargs)
        self.source = source
        self.bucket_name = bucket_name
        self.file_path = file_path
        self.file_name = file_name
        self.delimiter = delimiter
        self.encoding = encoding

    def execute(self, context):
        try:
            if len(self.source) > 0 and 'None' not in self.source:
                s3_key = f'artifacts/{self.file_path}/{self.file_name}'
                s3_region = Variable.get("CIE_AWS_REGION")
                access_id = Variable.get("SECRET_CIE_AWS_ID")
                secret_key = Variable.get("SECRET_CIE_AWS_KEY")
                bucket = self.bucket_name
                artifact_name = f'artifact/{self.file_path}/{self.file_name}'.replace(
                    "/", ":")
                self.log.info(
                    f"updating the file for artifact name {artifact_name} with s3-key {s3_key}")
                json_object = json.loads(self.source)
                df = pd.DataFrame(json_object)
                s3_client = boto3.client(service_name='s3',
                                         region_name=s3_region,
                                         aws_access_key_id=access_id,
                                         aws_secret_access_key=secret_key)
                df.to_csv(self.file_name, index=False)
                s3_client.upload_file(self.file_name, bucket, s3_key)
                return artifact_name
            return None
        except:
            return None
