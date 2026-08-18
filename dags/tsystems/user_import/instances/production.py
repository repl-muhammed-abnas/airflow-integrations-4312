from tsystems.user_import.config import *
from tsystems.user_import.mappers.login_status_mapper import login_status_mapper
from tsystems.user_import.mappers.time_zone_mapper import time_zone_mapper
from tsystems.user_import.mappers.permissions_mapper import permissions_mapper
from tsystems.user_import.mappers.activities_mapper import activities_mapper
from tsystems.user_import.mappers.employee_type_mapper import employee_type_mapper
from tsystems.user_import.mappers.timesheet_template_mapper import timesheet_template_mapper
from tsystems.user_import.mappers.time_off_type_mapper import time_off_type_mapper
from tsystems.user_import.mappers.defaults_mapper import defaults_mapper
from tsystems.user_import.mappers.oef_custom_mapper import oef_field_mapper, custom_field_mapper
from tsystems.user_import.mappers.api_keys_mapper import api_keys

# Override instance-specific settings
instance = 'prod'
environment = 'production'

company_key = "Tsystems"

# Company identification
replicon_conn_id = 'tsystems_replicon_repliconint.userimport'
http_conn_id = f'http_tsystems_caiman_{instance}'
sftp_conn_id = "sftp_tsystems_Replicon_Logs"

log_filepath = "/PROD/User Import"

tenant_email = 'TSI_Replicon@t-systems.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# DAG configuration
master_dag_id = f'tsystems_user_import_master_{instance}'
add_user_child_dag_id = f'tsystems_user_import_process_add_user_child_{instance}'
update_user_child_dag_id = f'tsystems_user_import_process_update_user_child_{instance}'
process_user_record_child_dag_id = f'tsystems_user_import_process_each_user_child_{instance}'
create_holiday_calendar_child_dag_id = f'tsystems_user_import_process_create_holiday_calendar_child_{instance}'
create_oef_tags_child_dag_id = f'tsystems_user_import_process_create_oef_tags_child_{instance}'
process_user_details_from_api_child_dag_id = f'tsystems_user_import_process_user_details_from_api_child_{instance}'
process_log_generation_child_dag_id = f'tsystems_user_import_process_log_generation_child_{instance}'

disable_users_child_dag_id = f'tsystems_user_import_disable_user_child_{instance}'
disable_users_master_dag_id = f'tsystems_user_import_disable_users_master_{instance}'

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
changed_since = ""
filter_query = "(&(objectClass=*)(|(tcid=11350522)(tcid=66798815)(tcid=93743757)(tcid=12238195)(tcid=30565025)(tcid=66798811)))"

can_use_user_api_source_var_name = f'tsystems_user_import_can_use_user_api_source_{instance}'
access_token = f"tsystems_caiman_access_token_variable_{instance}"
