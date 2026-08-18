#pylint: disable=wildcard-import unused-wildcard-import
from ascendmaterials.user_import.config import *

instance = 'trial'
environment = 'pre-production'

version = '' # '_v1 or _v2 etc.'

company_key = 'Ascendmaterialsafmig'

replicon_conn_id = 'ascendmaterials_trial_replicon_admin1'
sftp_conn_id = 'sftp_useast2'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

input_filepath = '/ascendmaterials/PROD/Users/Input'
reference_filepath = '/ascendmaterials/PROD/Users/Input/Reference'
archive_filepath = '/ascendmaterials/PROD/Users/Input/Archives/'
log_filepath = '/ascendmaterials/PROD/Users/Input/Logs/'

master_dag_id = f'ascend_user_import_master_{instance}{version}'
dept_costcenter_dag_id = f'ascend_child_dept_costcenter_{instance}{version}'
user_processor_dag_id = f'ascend_child_user_processor_{instance}{version}'
add_user_dag_id = f'ascend_child_add_user_{instance}{version}'
update_user_dag_id = f'ascend_child_update_user_{instance}{version}'
disable_user_dag_id = f'ascend_child_disable_user_{instance}{version}'
supervisor_dag_id = f'ascend_child_add_supervisor_{instance}{version}'
dynamic_wait_dag_id = f'ascend_dynamic_wait_{instance}{version}'
timeoff_add_dag_id = f'ascend_child_timeoff_add_{instance}{version}'
timeoff_update_dag_id = f'ascend_child_timeoff_update_{instance}{version}'
timeoff_policy_dag_id = f'ascend_child_timeoff_policy_{instance}{version}'
department_add_dag_id = f'ascend_sub_child_department_add_{instance}{version}'
cost_center_add_dag_id = f'ascend_sub_child_cost_center_add_{instance}{version}'

can_run_batch_task_var_name = f'ascendmaterials_{instance}_can_run_batch_task'