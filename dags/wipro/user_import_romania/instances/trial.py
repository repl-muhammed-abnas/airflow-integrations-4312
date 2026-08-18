# pylint: disable=wildcard-import unused-wildcard-import
from wipro.user_import_romania.config import *
from wipro.user_import_romania.mapper.general_mapper import role_approval_path_mapper
from wipro.user_import_romania.mapper.default_settings_mapper import user_default_settings
from wipro.user_import_romania.mapper.timeoff_mapper import time_off_types
from wipro.user_import_romania.mapper.timesheet_template_mapper import timesheet_template
instance = "trial"

region = 'eu-central-1'
environment = "pre-production"
time_zone = "Etc/UTC"
company_key = "Wiprosandbox2"

replicon_conn_id = "wiprosandbox2_replicon_myworkflow.Integration"

wipro_user_import_bearer_token_variable_trial = "wipro_user_import_bearer_token_variable_trial"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alerts_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_id=f"wipro_user_import_process_users_romania_master_{instance}"
create_location_dag_id=f"wipro_user_import_process_users_romania_create_location_child_{instance}"
create_supervisor_dag_id=f"wipro_user_import_process_users_romania_create_supervisor_child_{instance}"
add_user_dag_id=f"wipro_user_import_process_users_romania_add_child_{instance}"
update_user_dag_id=f"wipro_user_import_process_users_romania_update_child_{instance}"
valid_user_dag_id=f"wipro_user_import_process_valid_users_romania_child_{instance}"
disable_user_dag_id=f"wipro_disable_user_romania_{instance}"
log_schedule_dag_id = f"wipro_user_import_logs_romania_master_{instance}"

GENERAL_MAPPER = role_approval_path_mapper
DEFAULT_SETTINGS_MAPPER = user_default_settings
TIME_OFF_TYPES_MAPPER = time_off_types
TIMESHEET_TEMPLATE_MAPPER = timesheet_template
