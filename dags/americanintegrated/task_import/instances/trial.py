# pylint:disable=wildcard-import unused-wildcard-import
from americanintegrated.task_import.config import *
instance = "trial"
enviroment = "pre-production"
replicon_conn_id = "americanintegratedtrial01_replicon_admin"
company_key = "americanintegratedtrial01"
sftp_conn_id = "sftp_useast2"
sftp_task_file_path = "/americanintegrated/task/input/"
sftp_task_reference_path = "/americanintegrated/task/reference/"
sftp_task_archive_path = "/americanintegrated/task/archive/"
sftp_wages_file_path = "/americanintegrated/prevailingwages/input/"
sftp_wages_reference_path = "/americanintegrated/prevailingwages/reference/"
sftp_wages_archive_path = "/americanintegrated/prevailingwages/archive/"
sftp_log_file_path = "/americanintegrated/task/logs/"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
