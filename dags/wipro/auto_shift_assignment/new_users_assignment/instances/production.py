# pylint: disable=wildcard-import unused-wildcard-import
from wipro.auto_shift_assignment.new_users_assignment.config import *
from wipro.auto_shift_assignment.new_users_assignment.mapper.country_mapper_production import COUNTRY_MONTH_SHIFT_ASSIGNMENT_MAPPER_PROD

instance = 'production'

region = 'eu-central-1'
environment = 'production'

company_key = 'WiproLimited'
replicon_conn_id = 'WiproLimited_replicon_repliconint_shiftassignment'

tenant_email = 'replicon.log.ext@wipro.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

COUNTRY_MONTH_SHIFT_ASSIGNMENT = COUNTRY_MONTH_SHIFT_ASSIGNMENT_MAPPER_PROD


master_dag_id = f"wipro_auto_shift_assignment_new_users_master_{instance}"
child_dag_auto = f"wipro_auto_shift_assignment_new_users_shift_assignment_batch_child_{instance}"

can_run_batch_task_var_name = f'wipro_auto_shift_monthly_run_batch_task_{instance}'

# Country tuple used in SQL query filtering
country = ('Poland', 'Portugal', 'Spain')
