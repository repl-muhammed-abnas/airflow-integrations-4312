# pylint: disable=wildcard-import unused-wildcard-import
from wipro.user_import_portugal_v2.config import *
from wipro.user_import_portugal_v2.mapper.general_mapper import role_approval_path_mapper
from wipro.user_import_portugal_v2.mapper.default_settings_mapper import user_default_settings
from wipro.user_import_portugal_v2.mapper.timeoff_mapper import time_off_types
from wipro.user_import_portugal_v2.mapper.holiday_calendar_mapper import holiday_cal_mapper
from wipro.user_import_portugal_v2.mapper.leave_policy_balance_mapper import leave_policy_balance_mapper
from wipro.user_import_portugal_v2.mapper.new_entity_mapper import new_entity_path_mapper
from wipro.user_import_portugal_v2.mapper.new_entity_list import new_entity_list

instance = "trial"
environment = "pre-production"

company_key = "Wiprosandbox2"

replicon_conn_id = "wiprosandbox2_replicon_myworkflow.Integration"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alerts_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

version = "_v2"

master_dag_id = f"wipro_user_import_process_users_portugal_master_{instance}{version}"
create_location_dag_id = f"wipro_user_import_process_users_portugal_create_location_child_{instance}{version}"
create_supervisor_dag_id = f"wipro_user_import_process_users_portugal_supervisor_assignment_child_{instance}{version}"
add_user_dag_id = f"wipro_user_import_process_users_portugal_add_child_{instance}{version}"
update_user_dag_id = f"wipro_user_import_process_users_portugal_update_child_{instance}{version}"
valid_user_dag_id = f"wipro_user_import_process_valid_users_portugal_child_{instance}{version}"
log_schedule_dag_id = f"wipro_user_import_logs_portugal_master_{instance}"
add_new_entity_user_dag_id = f"wipro_user_import_process_new_entity_users_portugal_add_child_{instance}{version}"

GENERAL_MAPPER = role_approval_path_mapper
DEFAULT_SETTINGS_MAPPER = user_default_settings
TIME_OFF_TYPES_MAPPER = time_off_types
LEAVE_POLICY_BALANCE_MAPPER = leave_policy_balance_mapper
HOLIDAY_CALENDAR_MAPPER = holiday_cal_mapper
NEW_ENTITY_MAPPER = new_entity_path_mapper
NEW_ENTITY_LIST = new_entity_list
