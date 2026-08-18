# pylint: disable=wildcard-import unused-wildcard-import
from wipro.user_import_netherlands_v1.config import *
from wipro.user_import_netherlands_v1.mapper.general_mapper import role_approval_path_mapper
from wipro.user_import_netherlands_v1.mapper.default_settings_mapper import user_default_settings
from wipro.user_import_netherlands_v1.mapper.timeoff_mapper import time_off_types
from wipro.user_import_netherlands_v1.mapper.new_entity_list import new_entity_list

instance = "prod"

region = 'eu-central-1'
environment = "production"
time_zone = "Etc/UTC"
company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_replicon_repliconint"

wipro_user_import_bearer_token_variable_trial = "wipro_user_import_bearer_token_variable_prod"

version="_v1"
master_dag_id=f"wipro_user_import_process_users_netherlands_master_{instance}{version}"
create_location_dag_id=f"wipro_user_import_process_users_netherlands_create_location_child_{instance}{version}"
create_supervisor_dag_id=f"wipro_user_import_process_users_netherlands_create_supervisor_child_{instance}{version}"
add_user_dag_id=f"wipro_user_import_process_users_netherlands_add_child_{instance}{version}"
update_user_dag_id=f"wipro_user_import_process_users_netherlands_update_child_{instance}{version}"
valid_user_dag_id=f"wipro_user_import_process_valid_users_netherlands_child_{instance}{version}"
log_schedule_dag_id=f"wipro_user_import_logs_netherlands_master_{instance}"
add_new_entity_user_dag_id = f"wipro_user_import_process_new_entity_users_netherlands_add_child_{instance}{version}"

GENERAL_MAPPER = role_approval_path_mapper
DEFAULT_SETTINGS_MAPPER = user_default_settings
TIME_OFF_TYPES_MAPPER = time_off_types
NEW_ENTITY_LIST = new_entity_list