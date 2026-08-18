"""
UAT environment configuration for T-Systems Project Import integration - Version 1
This is V1.6 with all enhancements (zero padding, team assignment improvements)
"""
from tsystems.project_import_v1.config import *
from tsystems.project_import_v1.mapper.team_assigment_mapper import TEAM_ASSIGNMENT_MAPPING

instance = "uat"

version = "_v1"

team_assignment_mapper = TEAM_ASSIGNMENT_MAPPING

# Environment-specific settings
company_key = "tsystemsSB"
environment = "pre-production"

# File paths and SFTP configuration
log_filepath = '/TEST/Project Master Data Import'
replicon_conn_id = "tsystems_replicon_replicon.admin"
sftp_conn_id = "sftp_tsystems_Replicon_Logs"

create_projects_http_conn_id = f"tsystems_create_projects_api_{instance}"
update_projects_http_conn_id = f"tsystems_update_projects_api_{instance}"

access_token = f"tsystems_caiman_access_token_variable_{instance}"

create_project_endpoint = "/cost.object.create.event.v1/3ef40aa10526f19f287870af0cf2ca1ce29e5534"
update_project_endpoint = "/cost.object.update.event.v1/8f8d405612d546cc48f346865e119e037ee1259b"

# DAG IDs for trial environment
webhook_master_dag_id = f"tsystems_project_import_master_{instance}{version}"
process_payload_dag_id = f"tsystems_project_import_process_payload_{instance}{version}"
process_clients_dag_id = f"tsystems_project_import_process_clients_child_{instance}{version}"
process_each_record_dag_id = f"tsystems_project_import_process_each_record_child_{instance}{version}"
process_log_generation_dag_id = f"tsystems_project_import_process_log_generation_child_{instance}{version}"

# Authentication variables
can_run_batch_task_var_name = f"tsystems_project_import_can_run_batch_task_{instance}"

# Email configuration
tenant_email = "TSI_Replicon@t-systems.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled=True
