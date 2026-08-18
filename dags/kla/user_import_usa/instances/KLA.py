region = 'us-east-2'
environment = 'production'

instance = "KLA"

company_key = 'KLA'
replicon_conn_id = 'KLA-replicon-RNadmin'
can_run_batch_task_var_name = f'kla_user_import_usa_can_run_batch_task_{instance}'
# true only for qa testing . set this to false on prod - default false
can_use_conf_payload_var_name = f'kla_user_import_usa_can_use_conf_payload_{instance}'
http_conn_id = 'kla_user_import_usa_pdr'
aws_conn_id = 'replicon.workato_S3_account'
s3_key_name = "s3://replicon-integrations-uswest/KLA/userlogs"

execution_timeout_days = 14
child_dag_max_active_runs = 20
# Everyday at Eastern Time (US & Canada)  "hour": "05", "minute": "00"
schedule_interval = '0 5 * * *'
schedule_time_zone = 'EST'


tenant_email = 'Chris.cannon@kla.com,Jim.nordin@kla.com,DL-IT-Apps-Webapps@kla-tencor.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
user_import_report_name = '***User Import Reference'
disabled = True
