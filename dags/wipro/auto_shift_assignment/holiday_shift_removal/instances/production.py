# pylint: disable=wildcard-import unused-wildcard-import
from wipro.auto_shift_assignment.holiday_shift_removal.config import *
from wipro.auto_shift_assignment.holiday_shift_removal.mapper.country_mapper_production import COUNTRY_MONTH_SHIFT_ASSIGNMENT_MAPPER_PROD

instance = 'production'

region = 'eu-central-1'
environment = 'production'

company_key = 'WiproLimited'
replicon_conn_id = 'WiproLimited_replicon_repliconint_shiftassignment'

tenant_email = 'replicon.log.ext@wipro.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

COUNTRY_MONTH_SHIFT_ASSIGNMENT = COUNTRY_MONTH_SHIFT_ASSIGNMENT_MAPPER_PROD


process_each_country_dag_id = f"wipro_auto_shift_assignment_holiday_shift_removal_process_each_country_child_{instance}"
master_dag_id = f"wipro_auto_shift_assignment_holiday_shift_removal_master_{instance}"
delete_holiday_shift_assignment_dag_id = f"wipro_auto_shift_assignment_holiday_shift_removal_delete_assignment_batch_child_{instance}"

can_run_batch_task_var_name = f'wipro_auto_shift_holiday_shift_removal_run_batch_task_{instance}'
