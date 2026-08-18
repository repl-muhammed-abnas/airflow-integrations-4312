region = 'us-east-2'
environment = 'pre-production'

master_dag_active_runs = 1
child_dag_active_runs = 30

execution_timeout_days = 14

sumo_conn_id = 'sumologic-dagrunlogger'

pacific_timezone = 'America/Los_Angeles'

report_name = '***User Template - For User Sync'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
