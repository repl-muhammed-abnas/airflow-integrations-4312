import rail
from pendulum import datetime
from airflow.models import Variable


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.dag_id,
        description=config.dag_description,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.webhook_secret)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        upload_key = download_key = 'Test/file.txt'

        log_entry = rail.WriteLogOperator(
            task_id='log_entry',
            message="Testing the S3 Operator",
            severity='Exception',
            properties={
                'client': 'S3 Client',
                'project': 'S3 Project',
                'status': 'Success',
                'action': 'Validation',
                'details': "Testing the S3 Operator",
                'reference': 'S3 Log file testing',
                'exported': 'No'
            }
        )

        upload_file_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_file_to_s3',
            source='{{ result("log_entry") }}',
            key_name=upload_key,
            bucket_name=lambda: Variable.get(
                config.aws_s3_bucket, default_var='replicon-airflow-dev-group'),
            aws_conn_id=config.aws_conn_id,
            replace=True
        )

        download_file_from_s3 = rail.S3DownloadFileOperator(
            task_id='download_file_from_s3',
            bucket_name=lambda: Variable.get(
                config.aws_s3_bucket, default_var='replicon-airflow-dev-group'),
            key_name=download_key,
            aws_conn_id=config.aws_conn_id
        )

        log_entry >> upload_file_to_s3 >> download_file_from_s3

    return dag


rail.for_each_instance(create_dag)
