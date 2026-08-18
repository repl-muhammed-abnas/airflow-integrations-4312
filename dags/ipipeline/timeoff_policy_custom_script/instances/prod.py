# pylint: disable=wildcard-import unused-wildcard-import
from ipipeline.timeoff_policy_custom_script.config import *
from ipipeline.timeoff_policy_custom_script.mappers.accrual_rate_mapper import accrual_rate_mapper
from ipipeline.timeoff_policy_custom_script.mappers.type_mapper import timeoff_type_mapper

# Instance-specific settings
instance = 'production'
environment = 'production'
company_key = "iPipeline"

# Connection IDs
replicon_conn_id = 'ipipeline_replicon_repliconint.userimport'

tenant_email = 'hr@ipipeline.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version = '' # _v1
dag_id_suffix = f'{instance}{version}'

annual_run_master = f'ipipeline_timeoff_policy_custom_script_annual_run_master_{dag_id_suffix}'
daily_run_master = f'ipipeline_timeoff_policy_custom_script_daily_run_master_{dag_id_suffix}'
add_policyline_child = f'ipipeline_timeoff_policy_custom_script_child_{dag_id_suffix}'
process_log_generation = f'ipipeline_timeoff_policy_custom_script_process_log_generation_child_{dag_id_suffix}'

# Batch Control Variables
can_run_batch_task_var_name = f"ipipeline_timeoff_policy_custom_script_{dag_id_suffix}_can_run_batch_task"

ACCRUAL_RATE_MAPPER = accrual_rate_mapper
TIMEOFF_TYPE_MAPPER = timeoff_type_mapper
