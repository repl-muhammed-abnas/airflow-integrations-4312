
from tsystems.project_import_file_based_v3.config import *
from tsystems.project_import_file_based_v3.mapper.team_assigment_mapper import TEAM_ASSIGNMENT_MAPPING

instance = "uat"

team_assignment_mapper = TEAM_ASSIGNMENT_MAPPING

version = "_v3"

# Environment-specific settings
company_key = "tsystemsSB"
environment = "pre-production"

# File paths and SFTP configuration
log_filepath = '/TEST/Simunye_Cost Object/Logs/'
input_filepath = '/TEST/Simunye_Cost Object/'
archive_filepath = '/TEST/Simunye_Cost Object/archive'
replicon_conn_id = "tsystems_replicon_replicon.admin"
sftp_conn_id = "sftp_tsystems_filebased_Replicon_Logs"

file_based_master_dag_id = f"tsystems_project_import_file_based_master_{instance}{version}"
process_payload_dag_id = f"tsystems_project_import_file_based_process_payload_{instance}{version}"
process_clients_dag_id = f"tsystems_project_import_file_based_process_clients_child_{instance}{version}"
process_each_record_dag_id = f"tsystems_project_import_file_based_process_each_record_child_{instance}{version}"
process_log_generation_dag_id = f"tsystems_project_import_file_based_process_log_generation_child_{instance}{version}"
# Authentication variables
can_run_batch_task_var_name = f"tsystems_project_import_can_run_batch_task_{instance}"

# Email configuration
tenant_email = "TSI_Replicon@t-systems.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled=True
