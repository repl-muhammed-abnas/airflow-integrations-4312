region = 'us-east-1'
environment = 'pre-production'

instance = "MPMQafmig"

company_key = 'MPMQafmig'
replicon_conn_id = 'MPMQafmig_replicon_admin'

can_run_batch_task_var_name = f'mpmq_disable_users_can_run_batch_task_{instance}'

execution_timeout_days = 14
child_dag_max_active_runs = 20

#  "time_zone": "Beijing",
#  "hour": "18", - > 10AM UTC
schedule_interval = '0 10 * * *'

# "companykey": "MPMQ",

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
