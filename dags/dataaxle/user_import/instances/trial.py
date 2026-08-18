from dataaxle.user_import.config import *
from dataaxle.user_import.mappers.country_mapper import country_mapper
from dataaxle.user_import.mappers.location_and_timezone_mapper import location_and_timezone_mapper
from dataaxle.user_import.mappers.division_mapper import division_mapper

# instance
region = "us-east-1"
instance = "trial"
environment = "pre-production"
company_key = "dataaxleafmig"

# Connections
replicon_conn_id = f"{company_key}-replicon-radmin"

# Version
version = "" # eg: _v1, _v2
dag_suffix = f"{instance}{version}"

# Dag configuration
master_dag_id = f"dataaxle_user_import_master_{dag_suffix}"
child_create_job_title_dag_id = f"dataaxle_user_import_create_job_title_child_{dag_suffix}"
child_create_custom_fields_dag_id = f"dataaxle_user_import_create_custom_fields_child_{dag_suffix}"
child_create_office_schedule_dag_id = f"dataaxle_user_import_create_office_schedule_child_{dag_suffix}"
child_update_user_dag_id = f"dataaxle_user_import_update_user_child_{dag_suffix}"
child_create_user_supervisor_dag_id = f"dataaxle_user_import_create_user_supervisor_child_{dag_suffix}"
child_create_user_dag_id = f"dataaxle_user_import_create_user_child_{dag_suffix}"
child_process_users_dag_id = f"dataaxle_user_import_process_users_child_{dag_suffix}"

# SFTP
sftp_conn_id = "sftp_useast2"
input_file_path = "/dataaxle/user_import/input"
archive_file_path = "/dataaxle/user_import/archive"
reference_file_path = "/dataaxle/user_import/reference"
log_file_path = "/dataaxle/user_import/logs"


# Email
tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"

# Airflow variables
can_run_batch_task_var_name = f"dataaxle_user_import_can_run_batch_task_{instance}"


# Mapper
COUNTRY_MAPPER = country_mapper
LOCATION_AND_TIMEZONE_MAPPER = location_and_timezone_mapper
DIVISION_MAPPER = division_mapper