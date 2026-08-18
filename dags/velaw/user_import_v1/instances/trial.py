# pylint: disable=wildcard-import unused-wildcard-import
from velaw.user_import_v1.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'velawtrial01'
replicon_conn_id = 'velawtrial01_replicon_rintegrations'
sftp_conn_id = 'sftp_useast2'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


input_filepath = '/Velaw/user_import/Input'
reference_filepath = '/Velaw/user_import/Reference'
archive_filepath = '/Velaw/user_import/Archive'
log_filepath = '/Velaw/user_import/Logs'

can_run_batch_task_var_name = f'velaw_user_import_{instance}_can_run_batch_task'

# DAG IDs
master_dag_id = f'velaw_user_import_master_{instance}_v1'
add_user_child_dag_id = f'velaw_user_import_child_add_user_{instance}_v1'
cost_center_add_child_dag_id = f'velaw_user_import_child_cost_center_add_{instance}_v1'
department_add_child_dag_id = f'velaw_user_import_child_department_add_{instance}_v1'
division_add_child_dag_id = f'velaw_user_import_child_division_add_{instance}_v1'
employee_type_add_child_dag_id = f'velaw_user_import_child_employee_type_add_{instance}_v1'
location_add_child_dag_id = f'velaw_user_import_child_location_add_{instance}_v1'
groups_update_child_dag_id = f'velaw_user_import_child_groups_update_{instance}_v1'
supervisor_assignment_child_dag_id = f'velaw_user_import_child_supervisor_assignment_{instance}_v1'
workflow_to_disable_user_child_dag_id = f'velaw_user_import_child_workflow_to_disable_user_{instance}_v1'
user_update_child_dag_id = f'velaw_user_import_user_update_{instance}_v1'
timeoff_assignment_for_new_users_child_dag_id = f'velaw_user_import_child_timeoff_assignment_for_new_users_{instance}_v1'
timeoff_assignment_for_update_users_child_dag_id = f'velaw_user_import_child_timeoff_assignment_for_update_users_{instance}_v1'
timeoff_policy_update_for_no_accrual_child_dag_id = f'velaw_user_import_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_{instance}_v1'
type_policy_schedule_for_user_child_dag_id = f'velaw_user_import_time_off_type_policy_schedule_for_user_v1_0_{instance}_v1'
drop_down_udf_custom_field_check_dag_id = f'velaw_user_import_drop_down_udf_custom_field_check_{instance}_v1'
log_generation_child_dag_id = f'velaw_user_import_child_loggeneration_{instance}_v1'
