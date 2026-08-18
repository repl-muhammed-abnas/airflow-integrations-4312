# pylint: disable=wildcard-import unused-wildcard-import
from wipro.user_import_belgium_v1.config import *
from wipro.user_import_belgium_v1.mapper.general_mapper import role_approval_path_mapper
from wipro.user_import_belgium_v1.mapper.default_settings_mapper import user_default_settings
from wipro.user_import_belgium_v1.mapper.timeoff_mapper import time_off_types

instance = "trial"

region = 'eu-central-1'
environment = "pre-production"
time_zone = "Etc/UTC"
company_key = "Wiprosandbox2"

replicon_conn_id = "wiprosandbox2_replicon_myworkflow.Integration"

wipro_user_import_bearer_token_variable_trial = "wipro_user_import_bearer_token_variable_trial"

version="_v1"
master_dag_id=f"wipro_user_import_process_users_belgium_master_{instance}{version}"
create_location_dag_id=f"wipro_user_import_process_users_belgium_create_location_child_{instance}{version}"
create_supervisor_dag_id=f"wipro_user_import_process_users_belgium_create_supervisor_child_{instance}{version}"
add_user_dag_id=f"wipro_user_import_process_users_belgium_add_child_{instance}{version}"
update_user_dag_id=f"wipro_user_import_process_users_belgium_update_child_{instance}{version}"
valid_user_dag_id=f"wipro_user_import_process_valid_users_belgium_child_{instance}{version}"
log_schedule_dag_id=f"wipro_user_import_logs_belgium_master_{instance}"

GENERAL_MAPPER = role_approval_path_mapper
DEFAULT_SETTINGS_MAPPER = user_default_settings
TIME_OFF_TYPES_MAPPER = time_off_types
