from pwcglobal.user_import_v6.mapper.timeoff_approval_path_mapper import timeoff_approval_path_mapper
from pwcglobal.user_import_v6.mapper.language_mapper import language_mapper
from pwcglobal.user_import_v6.mapper.work_compliance_policy_mapper import work_compliance_policy_mapper

region = 'eu-central-1'
environment = 'pre-production'

user_report_name = "***User report***"

punch_entrypolicy_log_name = "user_import_punch_entrypolicy"

location_dag_max_active_runs = 1
schedule_dag_max_active_runs = 1
user_dag_max_active_runs = 15
supervisor_dag_max_active_runs = 10
dag_max_active_tasks = 10000
execution_timeout_days = 14
report_process_size = 5000

process_each_user_trigger_parallel_count = 50

secondary_log_filepath = ""

timeoff_approval_path_mapper = timeoff_approval_path_mapper
language_mapper = language_mapper
work_compliance_policy_mapper = work_compliance_policy_mapper
