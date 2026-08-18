# pylint: disable=wildcard-import unused-wildcard-import
from wipro.user_import_united_kingdom_v2.config import *
from wipro.user_import_united_kingdom_v2.mapper.general_mapper import role_approval_path_mapper
from wipro.user_import_united_kingdom_v2.mapper.company_code_specific_mapper import company_code_specific_mapper
from wipro.user_import_united_kingdom_v2.mapper.default_settings_mapper import user_default_settings
from wipro.user_import_united_kingdom_v2.mapper.timeoff_mapper import time_off_types
from wipro.user_import_united_kingdom_v2.mapper.new_entity_list import new_entity_list
from wipro.user_import_united_kingdom_v2.mapper.holiday_calender_mapper import holiday_calender_mapper

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
version="_v2"
master_dag_id = f"wipro_user_import_process_users_united_kingdom_master_{instance}{version}"
create_location_dag_id = f"wipro_user_import_process_users_united_kingdom_create_location_child_{instance}{version}"
create_supervisor_dag_id = f"wipro_user_import_process_users_united_kingdom_supervisor_assignment_child_{instance}{version}"
add_user_dag_id = f"wipro_user_import_process_users_united_kingdom_add_child_{instance}{version}"
update_user_dag_id = f"wipro_user_import_process_users_united_kingdom_update_child_{instance}{version}"
valid_user_dag_id = f"wipro_user_import_process_valid_users_united_kingdom_child_{instance}{version}"
disable_user_dag_id = f"wipro_disable_user_united_kingdom_{instance}"
log_schedule_dag_id = f"wipro_user_import_logs_united_kingdom_master_{instance}"
add_new_entity_user_dag_id = f"wipro_user_import_process_new_entity_users_united_kingdom_add_child_{instance}{version}"
GENERAL_MAPPER = role_approval_path_mapper
COMPANY_CODE_SPECIFIC_MAPPER= company_code_specific_mapper
DEFAULT_SETTINGS_MAPPER = user_default_settings
TIME_OFF_TYPES_MAPPER = time_off_types
NEW_ENTITY_LIST = new_entity_list
HOLIDAY_CALENDER_MAPPER = holiday_calender_mapper