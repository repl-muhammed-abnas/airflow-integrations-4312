from tsystems.user_import_v1.config import *
from tsystems.user_import_v1.mappers.login_status_mapper import login_status_mapper
from tsystems.user_import_v1.mappers.time_zone_mapper import time_zone_mapper
from tsystems.user_import_v1.mappers.permissions_mapper import permissions_mapper
from tsystems.user_import_v1.mappers.activities_mapper import activities_mapper
from tsystems.user_import_v1.mappers.employee_type_mapper import employee_type_mapper
from tsystems.user_import_v1.mappers.timesheet_template_mapper import timesheet_template_mapper
from tsystems.user_import_v1.mappers.time_off_type_mapper import time_off_type_mapper
from tsystems.user_import_v1.mappers.defaults_mapper import defaults_mapper
from tsystems.user_import_v1.mappers.oef_custom_mapper import oef_field_mapper, custom_field_mapper
from tsystems.user_import_v1.mappers.api_keys_mapper import api_keys

# Override instance-specific settings
instance = 'uat'
environment = 'pre-production'

company_key = "TsystemsSB"

# Version
version = "_v1" # _v1, _v2 etc.
dag_id_suffix = f"{instance}{version}"

# Company identification
replicon_conn_id = 'tsystems_replicon_replicon.admin'
http_conn_id = f'http_tsystemssb_caiman_{instance}'
sftp_conn_id = "sftp_tsystems_Replicon_Logs"

log_filepath = "/TEST/User Import"

tenant_email = 'TSI_Replicon@t-systems.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# DAG configuration
master_dag_id = f'tsystems_user_import_master_{dag_id_suffix}'
add_user_child_dag_id = f'tsystems_user_import_process_add_user_child_{dag_id_suffix}'
update_user_child_dag_id = f'tsystems_user_import_process_update_user_child_{dag_id_suffix}'
process_user_record_child_dag_id = f'tsystems_user_import_process_each_user_child_{dag_id_suffix}'
create_holiday_calendar_child_dag_id = f'tsystems_user_import_process_create_holiday_calendar_child_{dag_id_suffix}'
create_oef_tags_child_dag_id = f'tsystems_user_import_process_create_oef_tags_child_{dag_id_suffix}'
process_user_details_from_api_child_dag_id = f'tsystems_user_import_process_user_details_from_api_child_{dag_id_suffix}'
process_log_generation_child_dag_id = f'tsystems_user_import_process_log_generation_child_{dag_id_suffix}'
supervisor_assignment_child_dag_id = f'tsystems_user_import_process_supervisor_assignment_child_{dag_id_suffix}'

disable_users_child_dag_id = f'tsystems_user_import_disable_user_child_{dag_id_suffix}'
disable_users_master_dag_id = f'tsystems_user_import_disable_users_master_{dag_id_suffix}'

# Mapper variables
login_status_mapper_data = login_status_mapper
time_zone_mapper_data = time_zone_mapper
permissions_mapper_data = permissions_mapper
activities_mapper_data = activities_mapper
employee_type_mapper_data = employee_type_mapper
timesheet_template_mapper_data = timesheet_template_mapper
time_off_type_mapper_data = time_off_type_mapper
defaults_mapper_data = defaults_mapper
oef_field_mapper_data = oef_field_mapper
custom_field_mapper_data = custom_field_mapper
api_keys_mapper = api_keys

data_source = "CAIMAN"
data_source_stage = "SIT"
changed_since = "1h"
filter_query = "(&(objectClass=*)(|(tCostCenterAccountingArea=6207)(tCostCenterAccountingArea=0472)(tCostCenterAccountingArea=0370)(tTSISnumber=0472)(tTSISnumber=8108)(tTSISnumber=6543)(tTSISnumber=2380)))"

can_use_user_api_source_var_name = f'tsystems_user_import_can_use_user_api_source_{instance}'
access_token = f"tsystems_caiman_access_token_variable_{instance}"
