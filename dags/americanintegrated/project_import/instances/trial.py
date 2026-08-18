# pylint: disable=wildcard-import unused-wildcard-import
from americanintegrated.project_import.config import *
environment = "pre-production"
instance = "trial"
replicon_conn_id = "americanintegratedtrial01_replicon_admin"
company_key = "americanintegratedtrial01"
sftp_conn_id = "sftp_useast2"
sftp_import_file_path = "/americanintegrated/client_project/input"
sftp_archive_file_path = "/americanintegrated/client_project/archive/"
sftp_reference_file_path = "/americanintegrated/client_project/reference/"
sft_logs_file_path = "/americanintegrated/client_project/logs/"
sftp_task_file_path = "/americanintegrated/task/input"
sftp_wages_file_path = "/americanintegrated/prevailingwages/input"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_mail = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f"americanintegrated_project_import_master_{instance}"
process_client_dag_id = f"americanintegrated_project_import_process_each_client_child_{instance}"
process_task_dag_id = f"americanintegrated_project_import_create_task_child_{instance}"
process_project_dag_id = f"americanintegrated_project_import_process_each_project_child_{instance}"

disabled=True
