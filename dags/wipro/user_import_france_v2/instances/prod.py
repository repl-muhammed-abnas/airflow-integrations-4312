# pylint: disable=wildcard-import unused-wildcard-import
from wipro.user_import_france_v2.config import *
from wipro.user_import_france_v2.mapper.timeoff_mapper import time_off_types
from wipro.user_import_france_v2.mapper.default_settings_mapper import user_default_settings
from wipro.user_import_france_v2.mapper.time_off_balance_mapper import balance
from wipro.user_import_france_v2.mapper.general_mapper import role_approval_path_mapper

instance = "prod"

region = 'eu-central-1'
environment = "production"
time_zone = "Etc/UTC"
company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_replicon_repliconint"

wipro_user_import_bearer_token_variable_trial = "wipro_user_import_bearer_token_variable_prod"
tenant_email = "replicon.log.ext@wipro.com"
alerts_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

master_dag_id=f"wipro_user_import_process_users_france_master_{instance}_v2"
create_location_dag_id=f"wipro_user_import_process_users_france_create_location_child_{instance}_v2"
create_supervisor_dag_id=f"wipro_user_import_process_users_france_supervisor_assignment_child_{instance}_v2"
add_user_dag_id=f"wipro_user_import_process_users_france_add_child_{instance}_v2"
update_user_dag_id=f"wipro_user_import_process_users_france_update_child_{instance}_v2"
valid_user_dag_id=f"wipro_user_import_process_valid_users_france_child_{instance}_v2"
disable_user_dag_id=f"wipro_disable_user_france_{instance}"
log_schedule_dag_id=f"wipro_user_import_logs_france_master_{instance}"

GENERAL_MAPPER = role_approval_path_mapper
DEFAULT_SETTINGS_MAPPER = user_default_settings
TIME_OFF_TYPES_MAPPER = time_off_types
TIME_OFF_BALANCE_MAPPER = balance
