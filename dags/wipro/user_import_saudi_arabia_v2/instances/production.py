# pylint: disable=wildcard-import unused-wildcard-import
from wipro.user_import_saudi_arabia_v2.config import *
from wipro.user_import_saudi_arabia_v2.mapper.general_mapper import role_approval_path_mapper
from wipro.user_import_saudi_arabia_v2.mapper.default_settings_mapper import user_default_settings
from wipro.user_import_saudi_arabia_v2.mapper.timeoff_mapper import time_off_types

instance = "prod"

region = 'eu-central-1'
environment = "production"
time_zone = "Etc/UTC"
company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_replicon_repliconint"

wipro_user_import_bearer_token_variable_trial = "wipro_user_import_bearer_token_variable_prod"
version="_v2"
master_dag_id=f"wipro_user_import_process_users_saudi_arabia_master_{instance}{version}"
create_location_dag_id=f"wipro_user_import_process_users_saudi_arabia_create_location_child_{instance}{version}"
create_supervisor_dag_id=f"wipro_user_import_process_users_saudi_arabia_create_supervisor_child_{instance}{version}"
add_user_dag_id=f"wipro_user_import_process_users_saudi_arabia_add_child_{instance}{version}"
update_user_dag_id=f"wipro_user_import_process_users_saudi_arabia_update_child_{instance}{version}"
valid_user_dag_id=f"wipro_user_import_process_valid_users_saudi_arabia_child_{instance}{version}"
disable_user_dag_id=f"wipro_disable_user_saudi_arabia_{instance}"
log_schedule_dag_id = f"wipro_user_import_logs_saudi_arabia_master_{instance}"

GENERAL_MAPPER = role_approval_path_mapper
DEFAULT_SETTINGS_MAPPER = user_default_settings
TIME_OFF_TYPES_MAPPER = time_off_types
