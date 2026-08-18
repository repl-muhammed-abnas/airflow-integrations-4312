region = 'us-east-2'
environment = 'pre-production'

execution_timeout_days = 14
master_dag_max_active_runs = 1
child_dag_active_runs= 1
process_billing_rates_active_max_runs = 10

alert_email = '{{ var.value.dagrun_internal_testing_email }}'
schedule_interval = '7 15 * * *'

pacific_timezone = 'US/Pacific'
