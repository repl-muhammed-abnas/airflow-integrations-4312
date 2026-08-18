region = 'us-east-1'
environment = 'production'
instance = "IntercontinentalExchange"
company_key = 'IntercontinentalExchange'
replicon_conn_id = 'IntercontinentalExchange_replicon_admin'
can_run_batch_task_var_name = f'IntercontinentalExchange_timeoff_import_can_run_batch_task_{instance}'
user_report_name = '***UserDetails Report- Replicon Integration***'
sftp_conn_id = "sftp_IntercontinentalExchange_573892"
input_filepath = "/Production/Time Off/Input"
archive_filepath = "/Production/Time Off/Archive"
log_filepath = "/Production/Time Off/Log"

execution_timeout_days = 14
child_dag_max_active_runs = 10
master_dag_interval = 30

tenant_email = "ProdSupport-RepliconTimeManagement@theice.com,John.Cherian@ice.com,Angela.Davidson@ice.com,Mohammed.Shaban@ice.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
pacific_timezone = 'US/Pacific'
