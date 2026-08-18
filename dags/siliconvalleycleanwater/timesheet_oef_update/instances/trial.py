# pylint: disable=wildcard-import unused-wildcard-import
from siliconvalleycleanwater.timesheet_oef_update.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'siliconvalleycleanwaterafmig'
replicon_conn_id = 'siliconvalleycleanwaterafmig_replicon_admin'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'


can_run_batch_task_var_name = f'siliconvalleycleanwater_timesheet_oef_update_master_can_run_batch_task_{instance}'
disabled = True
