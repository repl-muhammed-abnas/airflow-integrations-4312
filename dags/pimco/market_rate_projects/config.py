region = 'us-east-1'
environment = 'pre-production'

company_key = 'PIMCOTrial01'
replicon_conn_id = 'pimco-replicon-trial'
sftp_conn_id = 'sftp_useast2'

max_active_runs_process_child = 5
process_market_rate_update_child_count = 20
execution_timeout_days = 14
master_dag_interval = '0 22 * * *'
pacific_timezone = 'America/Los_Angeles'

extract_task_report_name = "Model project - task details"
extract_project_report_name = "**ALL Project details**"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
