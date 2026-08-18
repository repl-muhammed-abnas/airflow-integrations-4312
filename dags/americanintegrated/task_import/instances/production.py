# pylint:disable=wildcard-import unused-wildcard-import
from americanintegrated.task_import.config import *
environment = "production"
instance = "production"
company_key = 'AmericanIntegrated'
replicon_conn_id = 'americanintegrated_project_import'
sftp_conn_id = "sftp_uswest_647462"

sftp_task_file_path = "/task/input"
sftp_task_reference_path = "/task/reference/"
sftp_task_archive_path = "/task/archive/"
sftp_wages_file_path = "/prevailingwages/input"
sftp_wages_reference_path = "/prevailingwages/reference/"
sftp_wages_archive_path = "/prevailingwages/archive/"
sftp_log_file_path = "/task/logs/"

tenant_email = 'andelgado@americanintegrated.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
