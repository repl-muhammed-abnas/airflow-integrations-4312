region = 'us-east-2'
environment = 'pre-production'

alert_email = '{{ var.value.dagrun_internal_testing_email }}'

schedule_interval = '0 */2 * * *'

execution_timeout_days = 14
master_dag_max_active_runs = 1
child_dag_active_runs= 3
