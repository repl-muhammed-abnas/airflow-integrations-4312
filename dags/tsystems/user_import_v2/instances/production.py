from tsystems.user_import_v2.config import *
from tsystems.user_import_v2.mappers.login_status_mapper import login_status_mapper
from tsystems.user_import_v2.mappers.time_zone_mapper import time_zone_mapper
from tsystems.user_import_v2.mappers.permissions_mapper import permissions_mapper
from tsystems.user_import_v2.mappers.activities_mapper import activities_mapper
from tsystems.user_import_v2.mappers.employee_type_mapper import employee_type_mapper
from tsystems.user_import_v2.mappers.timesheet_template_mapper import timesheet_template_mapper
from tsystems.user_import_v2.mappers.time_off_type_mapper import time_off_type_mapper
from tsystems.user_import_v2.mappers.defaults_mapper import defaults_mapper
from tsystems.user_import_v2.mappers.oef_custom_mapper import oef_field_mapper, custom_field_mapper
from tsystems.user_import_v2.mappers.api_keys_mapper import api_keys

# Override instance-specific settings
instance = 'prod'
environment = 'production'

company_key = "Tsystems"

# Version
version = "_v2" # _v1, _v2 etc.
dag_id_suffix = f"{instance}{version}"

# Company identification
replicon_conn_id = 'tsystems_replicon_repliconint.userimport'
http_conn_id = f'http_tsystems_caiman_{instance}'
sftp_conn_id = "sftp_tsystems_Replicon_Logs"

log_filepath = "/PROD/User Import"

tenant_email = 'TSI_Replicon@t-systems.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# DAG configuration
master_dag_id = f'tsystems_user_import_master_{dag_id_suffix}'
add_user_child_dag_id = f'tsystems_user_import_process_add_user_child_{dag_id_suffix}'
update_user_child_dag_id = f'tsystems_user_import_process_update_user_child_{dag_id_suffix}'
process_user_record_child_dag_id = f'tsystems_user_import_process_each_user_child_{dag_id_suffix}'
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
data_source_stage = "PROD"
changed_since = "1h"
filter_query = "(&(objectClass=*)(|(tCostCenterAccountingArea=0010)(tCostCenterAccountingArea=0030)(tCostCenterAccountingArea=0070)(tCostCenterAccountingArea=0150)(tCostCenterAccountingArea=0151)(tCostCenterAccountingArea=0152)(tCostCenterAccountingArea=0153)(tCostCenterAccountingArea=0154)(tCostCenterAccountingArea=0155)(tCostCenterAccountingArea=0156)(tCostCenterAccountingArea=0170)(tCostCenterAccountingArea=0193)(tCostCenterAccountingArea=0250)(tCostCenterAccountingArea=0330)(tCostCenterAccountingArea=0350)(tCostCenterAccountingArea=0353)(tCostCenterAccountingArea=0370)(tCostCenterAccountingArea=0377)(tCostCenterAccountingArea=0382)(tCostCenterAccountingArea=0385)(tCostCenterAccountingArea=0386)(tCostCenterAccountingArea=0387)(tCostCenterAccountingArea=0388)(tCostCenterAccountingArea=0430)(tCostCenterAccountingArea=0450)(tCostCenterAccountingArea=0472)(tCostCenterAccountingArea=0490)(tCostCenterAccountingArea=0820)(tCostCenterAccountingArea=0830)(tCostCenterAccountingArea=0850)(tCostCenterAccountingArea=0880)(tCostCenterAccountingArea=1046)(tCostCenterAccountingArea=1048)(tCostCenterAccountingArea=1183)(tCostCenterAccountingArea=1440)(tCostCenterAccountingArea=1539)(tCostCenterAccountingArea=1709)(tCostCenterAccountingArea=2600)(tCostCenterAccountingArea=2641)(tCostCenterAccountingArea=2804)(tCostCenterAccountingArea=6201)(tCostCenterAccountingArea=6205)(tCostCenterAccountingArea=6206)(tCostCenterAccountingArea=6207)(tCostCenterAccountingArea=6208)(tCostCenterAccountingArea=6209)(tCostCenterAccountingArea=6210)(tCostCenterAccountingArea=6229)(tCostCenterAccountingArea=8344)(tCostCenterAccountingArea=9973)(tCostCenterAccountingArea=2654)))"

can_use_user_api_source_var_name = f'tsystems_user_import_can_use_user_api_source_{instance}'
can_run_batch_task_var_name = f'tsystems_can_run_batch_task_{instance}'
access_token = f"tsystems_caiman_access_token_variable_{instance}"

# Performance settings for different processes
max_active_runs = 1
process_user_child_max_active_runs = 5
add_user_child_max_active_runs = 5
update_user_child_max_active_runs = 5
process_user_details_from_api_child_max_active_runs = 5
create_oef_tags_child_max_active_runs = 5
process_log_generation_child_max_active_runs = 1
disable_user_child_max_active_runs = 5
disable_user_master_max_active_runs = 1
supervisor_assignment_child_max_active_runs = 5
execution_timeout_days = 1
gather_logs_timeout_hours = 4
gather_user_details_timeout_hours = 4
