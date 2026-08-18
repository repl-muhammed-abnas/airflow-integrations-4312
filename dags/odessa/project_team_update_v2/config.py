region = 'us-east-1'
environment = 'pre-production'

schedule_interval = '0 */1 * * *'
pacific_timezone = 'America/Los_Angeles'

end_date = "12/31/2099"

execution_timeout_days = 14
master_dag_max_active_runs = 1
child_dag_process_wbs_max_active_runs = 14
second_master_dag_max_active_runs=6

error_email =  '{{ var.value.dagrun_internal_log_email }}'
internal_logs_email =  '{{ var.value.dagrun_internal_testing_email }}'
is_update_custom_field_in_jira= False
