region = 'us-east-2'
environment = 'pre-production'

instance = "trial"

company_key = 'technicolorg3afmig'
replicon_conn_id = 'replicon-technicolorg3afmig-admin'
webhook_secret = f'technicolorg3_schedule_data_import_webhook_secret_{instance}'
lookup_log_timestamp_var = f'technicolorg3_schedule_data_import_log_timestamp_{instance}'
can_run_batch_task_var_name = f'technicolorg3_schedule_data_import_can_run_batch_task_{instance}'
sftp_conn_id = "sftp_technicolorg3afmig_schedule_data_import"

execution_timeout_days = 14
master_dag_max_active_runs = 20
child_dag_max_active_runs = 20

log_generation_dag_interval = '0 */3 * * *'  # every 3 hours
log_filepath = "/technicolorg3afmig/schedule_data_import/logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disable=True

disabled=True
