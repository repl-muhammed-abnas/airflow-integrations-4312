# pylint: disable=wildcard-import unused-wildcard-import
from americanintegrated.project_import.config import *
environment = "production"
instance = "production"
company_key = 'AmericanIntegrated'
replicon_conn_id = 'americanintegrated_project_import'
sftp_conn_id = "sftp_uswest_647462"

sftp_import_file_path = "/clientproject/input"
sftp_archive_file_path = "/clientproject/archive/"
sftp_reference_file_path = "/clientproject/reference/"
sft_logs_file_path = "/clientproject/logs/"
sftp_task_file_path = "/task/input"
sftp_wages_file_path = "/prevailingwages/input"

tenant_email = 'andelgado@americanintegrated.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_mail = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f"americanintegrated_project_import_master_{instance}"
process_client_dag_id = f"americanintegrated_project_import_process_each_client_child_{instance}"
process_task_dag_id = f"americanintegrated_project_import_create_task_child_{instance}"
process_project_dag_id = f"americanintegrated_project_import_process_each_project_child_{instance}"
