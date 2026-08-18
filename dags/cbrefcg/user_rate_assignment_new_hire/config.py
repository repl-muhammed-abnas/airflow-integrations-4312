region = 'us-east-2'
environment = 'pre-production'

schedule_interval = '7 15 * * *'
pacific_timezone = 'US/Pacific'

alert_email = '{{ var.value.dagrun_internal_testing_email }}'

master_dag_max_active_runs = 1
child_dag_active_runs= 1
execution_timeout_days = 14
