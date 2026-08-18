region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'siliconvalleycleanwaterafmig'
replicon_conn_id = 'siliconvalleycleanwaterafmig_replicon_admin'

can_run_batch_task_var_name = f'siliconvalleycleanwater_workorder_sync_can_run_batch_task_{instance}'

sftp_conn_id = 'sftp_klatrial_schedule_data_import'

execution_timeout_days = 14
child_dag_max_active_runs = 20
webhook_shared_secrete = f"siliconvalleycleanwater_workorder_sync_webhooks_secrete_{instance}"
# "email": "nexinitedev@nexinite.com"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
