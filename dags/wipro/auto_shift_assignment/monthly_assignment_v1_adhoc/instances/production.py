# pylint: disable=wildcard-import unused-wildcard-import
from wipro.auto_shift_assignment.monthly_assignment_v1_adhoc.config import *
from wipro.auto_shift_assignment.monthly_assignment_v1_adhoc.mapper.country_mapper_production import COUNTRY_MONTH_SHIFT_ASSIGNMENT_MAPPER_PROD

instance = 'production'

region = 'eu-central-1'
environment = 'production'

company_key = 'WiproLimited'  # Update with actual production company key
replicon_conn_id = 'WiproLimited_replicon_repliconint_shiftassignment'  # Update with actual production connection

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

COUNTRY_MONTH_SHIFT_ASSIGNMENT = COUNTRY_MONTH_SHIFT_ASSIGNMENT_MAPPER_PROD

version = "_v1_adhoc"
child_dag_id = f"wipro_auto_shift_assignment_monthly_process_each_country_child_{instance}{version}"
master_dag_id = f"wipro_auto_shift_assignment_monthly_master_{instance}{version}"
child_dag_auto = f"wipro_auto_shift_assignment_monthly_users_shift_assignment_batch_child_{instance}{version}"

can_run_batch_task_var_name = f'wipro_auto_shift_monthly_run_batch_task_{instance}'
