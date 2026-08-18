# pylint: disable=wildcard-import unused-wildcard-import
from siliconvalleycleanwater.timesheet_oef_update.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'SiliconValleyCleanWater'
replicon_conn_id = 'SiliconValleyCleanWater_replicon_admin'

tenant_email = 'vishwaram@svcw.org'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'


can_run_batch_task_var_name = f'siliconvalleycleanwater_timesheet_oef_update_master_can_run_batch_task_{instance}'
