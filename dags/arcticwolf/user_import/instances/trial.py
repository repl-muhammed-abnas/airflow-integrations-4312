from arcticwolf.user_import.config import *
from arcticwolf.user_import.mapper.user_import_mapper import user_import_mapper

instance = 'trial'
environment = 'pre-production'
company_key = 'arcticwolfnetworksinctrial02'
replicon_conn_id = 'arcticwolfnetworksinctrial02_replicon_admin'
workday_http_conn_id = 'arcticwolf_user_import_workday_http_connection'
sftp_conn_id = 'sftp_useast2'


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

log_filepath = '/arcticwolf/user_import/logs/'
reference_filepath = '/arcticwolf/user_import/reference/'
archive_filepath = '/arcticwolf/user_import/archive/'


can_run_batch_task = f'arctic_wolf_user_import_can_run_batch_task_var_name_{instance}'

master_dagid = f'arcticwolf_user_import_master_{instance}'
user_add_child_dagid = f'arcticwolf_user_add_child_{instance}'
user_update_child_dagid = f'arcticwolf_user_update_child_{instance}'
cost_center_groups_add_child_dagid = f'arcticwolf_cost_center_groups_add_child_{instance}'
divisiongroups_add_child_dagid = f'arcticwolf_divisiongroups_add_child_{instance}'
employeetypegroups_add_child_dagid = f'arcticwolf_employeetypegroups_add_child_{instance}'
groups_update_child_dagid = f'arcticwolf_groups_update_child_{instance}'
position_title_groups_add_child_dagid = f'arcticwolf_position_title_groups_add_child_{instance}'
department_add_child_dagid = f'arcticwolf_department_add_child_{instance}'
location_add_child_dagid = f'arcticwolf_location_add_child_{instance}'
assign_supervisor_child_dagid = f'arcticwolf_assign_supervisor_child_{instance}'

USER_IMPORT_MAPPER = user_import_mapper

disabled = True
