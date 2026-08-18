region = 'us-east-1'
environment = 'production'

instance = "horizonmedia"

company_key = 'horizonmedia'
replicon_conn_id = 'horizonmedia_repliconadmin_replicon'

can_run_batch_task_var_name = f'horizonmedia_user_import_can_run_batch_task_{instance}'

base_report_name = 'BaseReport_SupervisorORG_Group_Assignment'

execution_timeout_days = 14
child_dag_max_active_runs = 20

schedule_time_zone = 'EST'
schedule_interval = '30 8 * * 1-5'

tenant_email = "gfraga@horizonmedia.com,Sgrandi@horizonmedia.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
