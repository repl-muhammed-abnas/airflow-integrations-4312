region = 'us-east-1'
environment = 'production'

instance = "Momentive"

company_key = 'Momentive'
replicon_conn_id = 'momentive-replicon-admin'

can_run_batch_task_var_name = f'momentive_disable_users_can_run_batch_task_{instance}'

execution_timeout_days = 14
child_dag_max_active_runs = 20

#  "time_zone": "IST",
#  6 pm IST everyday - > 12 30 PM UTC
schedule_interval = '30 12 * * *'

tenant_email = '{{ var.value.dagrun_internal_log_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
