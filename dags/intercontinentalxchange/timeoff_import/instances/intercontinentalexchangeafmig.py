region = 'us-east-1'
environment = 'pre-production'
instance = "IntercontinentalExchangeafmig"
company_key = 'IntercontinentalExchangeafmig'
replicon_conn_id = 'IntercontinentalExchange_User_Import'
can_run_batch_task_var_name = f'IntercontinentalExchange_timeoff_import_can_run_batch_task_{instance}'
user_report_name = '***UserDetails Report- Replicon Integration***'
sftp_conn_id = "sftp_useast2"
input_filepath = "/IntercontinentalExchangeafmig/Timeoff_Import/Input"
referance_filepath = "/IntercontinentalExchangeafmig/Timeoff_Import/Reference"
archive_filepath = "/IntercontinentalExchangeafmig/Timeoff_Import/Archive"
log_filepath = "/IntercontinentalExchangeafmig/Timeoff_Import/Logs"

execution_timeout_days = 14
child_dag_max_active_runs = 10
master_dag_interval = 30

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
pacific_timezone = 'US/Pacific'
schedule_interval_daily = '0 22 * * *'


disabled=True
