region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'siliconvalleycleanwater'
replicon_conn_id = 'SiliconValleyCleanWater_replicon_admin'

can_run_batch_task_var_name = f'siliconvalleycleanwater_project_sync_can_run_batch_task_{instance}'

sftp_conn_id = 'sftp_SiliconValleyCleanWater_667271'

execution_timeout_days = 14
child_dag_max_active_runs = 20
webhook_shared_secrete = f"siliconvalleycleanwater_project_sync_webhooks_secrete_{instance}"

tenant_email = "nexinitedev@nexinite.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
