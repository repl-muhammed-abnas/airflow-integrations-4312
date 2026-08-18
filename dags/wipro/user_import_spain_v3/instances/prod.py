# pylint: disable=wildcard-import unused-wildcard-import
from wipro.user_import_spain_v3.config import *
from wipro.user_import_spain_v3.mapper.general_mapper import role_approval_path_mapper
from wipro.user_import_spain_v3.mapper.default_settings_mapper import user_default_settings
from wipro.user_import_spain_v3.mapper.timeoff_mapper import time_off_types
from wipro.user_import_spain_v3.mapper.new_entity_mapper import new_entity_path_mapper
from wipro.user_import_spain_v3.mapper.new_entity_list import new_entity_list

instance = "prod"

region = 'eu-central-1'
environment = "production"
time_zone = "Etc/UTC"
company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_replicon_repliconint"

wipro_user_import_bearer_token_variable_trial = "wipro_user_import_bearer_token_variable_prod"

version = "v3"

master_dag_id=f"wipro_user_import_process_users_spain_master_{instance}_{version}"
create_location_dag_id=f"wipro_user_import_process_users_spain_create_location_child_{instance}_{version}"
create_supervisor_dag_id=f"wipro_user_import_process_users_spain_supervisor_assignment_child_{instance}_{version}"
add_user_dag_id=f"wipro_user_import_process_users_spain_add_child_{instance}_{version}"
update_user_dag_id=f"wipro_user_import_process_users_spain_update_child_{instance}_{version}"
valid_user_dag_id=f"wipro_user_import_process_valid_users_spain_child_{instance}_{version}"
log_schedule_dag_id=f"wipro_user_import_logs_spain_master_{instance}"
add_new_entity_user_dag_id = f"wipro_user_import_process_new_entity_users_spain_add_child_{instance}_{version}"

GENERAL_MAPPER = role_approval_path_mapper
DEFAULT_SETTINGS_MAPPER = user_default_settings
TIME_OFF_TYPES_MAPPER = time_off_types
NEW_ENTITY_MAPPER = new_entity_path_mapper
NEW_ENTITY_LIST = new_entity_list
