# pylint: disable=wildcard-import unused-wildcard-import
from wipro.user_import_switzerland.config import *
from wipro.user_import_switzerland.mapper.general_mapper import role_approval_path_mapper
from wipro.user_import_switzerland.mapper.default_settings_mapper import user_default_settings
from wipro.user_import_switzerland.mapper.timeoff_mapper import time_off_types
from wipro.user_import_switzerland.mapper.holiday_calendar_mapper import holiday_calendar

instance = "prod"

region = 'eu-central-1'
environment = "production"
time_zone = "Etc/UTC"
company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_replicon_repliconint"

wipro_user_import_bearer_token_variable_trial = "wipro_user_import_bearer_token_variable_prod"


master_dag_id = f"wipro_user_import_process_users_switzerland_master_{instance}"
create_location_dag_id = f"wipro_user_import_process_users_switzerland_create_location_child_{instance}"
create_supervisor_dag_id = f"wipro_user_import_process_users_switzerland_supervisor_assignment_child_{instance}"
add_user_dag_id = f"wipro_user_import_process_users_switzerland_add_child_{instance}"
update_user_dag_id = f"wipro_user_import_process_users_switzerland_update_child_{instance}"
valid_user_dag_id = f"wipro_user_import_process_valid_users_switzerland_child_{instance}"
disable_user_dag_id = f"wipro_disable_user_switzerland_{instance}"
log_schedule_dag_id = f"wipro_user_import_logs_switzerland_master_{instance}"

GENERAL_MAPPER = role_approval_path_mapper
DEFAULT_SETTINGS_MAPPER = user_default_settings
TIME_OFF_TYPES_MAPPER = time_off_types
HOLIDAY_CALENDAR_MAPPER = holiday_calendar