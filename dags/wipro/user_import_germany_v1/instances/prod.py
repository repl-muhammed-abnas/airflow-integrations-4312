# pylint: disable=wildcard-import unused-wildcard-import
from wipro.user_import_germany_v1.config import *
from wipro.user_import_germany_v1.mapper.general_mapper import role_approval_path_mapper
from wipro.user_import_germany_v1.mapper.default_settings_mapper import user_default_settings
from wipro.user_import_germany_v1.mapper.timeoff_mapper import time_off_types
from wipro.user_import_germany_v1.mapper.holiday_calendar_mapper import holiday_calendar
from wipro.user_import_germany_v1.mapper.new_entity_path_mapper import new_entity_path_mapper
from wipro.user_import_germany_v1.mapper.new_entity_list import new_entity_list

instance = "prod"

region = 'eu-central-1'
environment = "production"
time_zone = "Etc/UTC"
company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_replicon_repliconint"

wipro_user_import_bearer_token_variable_trial = "wipro_user_import_bearer_token_variable_prod"
tenant_email = 'replicon.log.ext@wipro.com'
alerts_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
version="_v1"
master_dag_id=f"wipro_user_import_process_users_germany_master_{instance}{version}"
create_location_dag_id=f"wipro_user_import_process_users_germany_create_location_child_{instance}{version}"
create_supervisor_dag_id=f"wipro_user_import_process_users_germany_create_supervisor_child_{instance}{version}"
add_user_dag_id=f"wipro_user_import_process_users_germany_add_child_{instance}{version}"
update_user_dag_id=f"wipro_user_import_process_users_germany_update_child_{instance}{version}"
valid_user_dag_id=f"wipro_user_import_process_valid_users_germany_child_{instance}{version}"
disable_user_dag_id=f"wipro_disable_user_germany_{instance}"
log_schedule_dag_id=f"wipro_user_import_logs_germany_master_{instance}"
add_new_entity_user_dag_id = f"wipro_user_import_process_new_entity_users_germany_add_child_{instance}{version}"

GENERAL_MAPPER = role_approval_path_mapper
DEFAULT_SETTINGS_MAPPER = user_default_settings
TIME_OFF_TYPES_MAPPER = time_off_types
HOLIDAY_CALENDAR_MAPPER = holiday_calendar
NEW_ENTITY_MAPPER = new_entity_path_mapper
NEW_ENTITY_LIST = new_entity_list
EMPLOYEE_BAND_FOR_TIMEOFF_APPROVAL_PATH = ["GROUP D1","GROUP D2","GROUP E"]
TIMEOFF_APPROVAL_PATH_BASED_ON_EMPLOYEE_BAND = "Germany Approval Leadership Level"
