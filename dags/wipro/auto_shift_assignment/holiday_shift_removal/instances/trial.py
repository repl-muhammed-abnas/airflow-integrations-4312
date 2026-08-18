# pylint: disable=wildcard-import unused-wildcard-import
from wipro.auto_shift_assignment.holiday_shift_removal.config import *
from wipro.auto_shift_assignment.holiday_shift_removal.mapper.country_mapper import COUNTRY_MONTH_SHIFT_ASSIGNMENT_MAPPER

instance = 'trial'

region = 'eu-central-1'
environment = 'pre-production'

company_key = 'Wiprosandbox2'
replicon_conn_id = 'Wiprosandbox2_replicon_repliconint'

tenant_email = 'replicon.log.ext@wipro.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

COUNTRY_MONTH_SHIFT_ASSIGNMENT = COUNTRY_MONTH_SHIFT_ASSIGNMENT_MAPPER


process_each_country_dag_id = f"wipro_auto_shift_assignment_holiday_shift_removal_process_each_country_child_{instance}"
master_dag_id = f"wipro_auto_shift_assignment_holiday_shift_removal_master_{instance}"
delete_holiday_shift_assignment_dag_id = f"wipro_auto_shift_assignment_holiday_shift_removal_delete_assignment_batch_child_{instance}"

can_run_batch_task_var_name = f'wipro_auto_shift_holiday_shift_removal_run_batch_task_{instance}'
