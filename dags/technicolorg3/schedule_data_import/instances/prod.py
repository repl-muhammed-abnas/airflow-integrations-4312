region = 'us-east-2'
environment = 'production'

instance = "production"

company_key = 'TechnicolorG3'
replicon_conn_id = 'replicon-technicolorG3-admin'
webhook_secret = f'technicolorg3_schedule_data_import_webhook_secret_{instance}'
lookup_log_timestamp_var = f'technicolorg3_schedule_data_import_log_timestamp_{instance}'
can_run_batch_task_var_name = f'technicolorg3_schedule_data_import_can_run_batch_task_{instance}'

execution_timeout_days = 14
master_dag_max_active_runs = 20
child_dag_max_active_runs = 20

log_generation_dag_interval = '0 */3 * * *'  # every 3 hours

tenant_email = "psadvreplicon-support@technicolor.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
