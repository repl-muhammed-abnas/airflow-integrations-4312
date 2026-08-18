# pylint: disable=wildcard-import unused-wildcard-import
from wipro.auto_shift_assignment.monthly_assignment_v1.config import *
from wipro.auto_shift_assignment.monthly_assignment_v1.mapper.country_mapper import COUNTRY_MONTH_SHIFT_ASSIGNMENT_MAPPER

instance = 'uat'

region = 'eu-central-1'
environment = 'pre-production'

company_key = 'Wiprosandbox2'
replicon_conn_id = 'Wiprosandbox2_replicon_repliconint'


tenant_email = 'replicon.log.ext@wipro.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


COUNTRY_MONTH_SHIFT_ASSIGNMENT = COUNTRY_MONTH_SHIFT_ASSIGNMENT_MAPPER

version = "_v1"
child_dag_id = f"wipro_auto_shift_assignment_monthly_process_each_country_child_{instance}{version}"
master_dag_id = f"wipro_auto_shift_assignment_monthly_master_{instance}{version}"
child_dag_auto = f"wipro_auto_shift_assignment_monthly_users_shift_assignment_batch_child_{instance}{version}"

can_run_batch_task_var_name = f'wipro_auto_shift_monthly_run_batch_task_{instance}'
