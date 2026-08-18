region = 'us-east-1'
environment = 'pre-production'

child_dag_max_active_runs = 16
max_active_dag_runs = 1
execution_timeout_days = 14
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
user_report_name = "user details - termination balance"
termination_balance_report_name = "Termination balance report"
