# pylint: disable=wildcard-import unused-wildcard-import
from ipipeline.timeoff_policy_custom_script.config import *
from ipipeline.timeoff_policy_custom_script.mappers.accrual_rate_mapper import accrual_rate_mapper
from ipipeline.timeoff_policy_custom_script.mappers.type_mapper import timeoff_type_mapper

instance = "trial"

# Instance Identification
company_key = "iPipelineSB"

# Connection IDs
replicon_conn_id = 'ipipelinesb_replicon_repliconint.userimport'

annual_run_master = f'ipipeline_timeoff_policy_custom_script_annual_run_master_{instance}'
daily_run_master = f'ipipeline_timeoff_policy_custom_script_daily_run_master_{instance}'
add_policyline_child = f'ipipeline_timeoff_policy_custom_script_child_{instance}'
process_log_generation = f'ipipeline_timeoff_policy_custom_script_process_log_generation_child_{instance}'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f"ipipeline_timeoff_policy_custom_script_{instance}_can_run_batch_task"

ACCRUAL_RATE_MAPPER = accrual_rate_mapper
TIMEOFF_TYPE_MAPPER = timeoff_type_mapper
