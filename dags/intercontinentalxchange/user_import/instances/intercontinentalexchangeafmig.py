region = 'us-east-1'
environment = 'pre-production'
instance = "IntercontinentalExchangeafmig"
company_key = 'IntercontinentalExchangeafmig'
replicon_conn_id = 'IntercontinentalExchange_User_Import'
can_run_batch_task_var_name = f'IntercontinentalExchange_project_task_import_can_run_batch_task_{instance}'
user_report_name = 'User list - For Integration'
user_report_to_disable = "Enabled User list - For Disabling users"
user_managerhierarchy_report = "Managerhierarchy_Basereport"
sftp_conn_id = "sftp_IntercontinentalExchange_schedule_data_import"
input_filepath = "/IntercontinentalExchangeafmig/Input"
referance_filepath = "/IntercontinentalExchangeafmig/Reference"
archive_filepath = "/IntercontinentalExchangeafmig/Archive"

manage_hierarhy_input_filepath = "/IntercontinentalExchangeafmig/Manager Hierarchy/Input"
manage_hierarhy_referance_filepath = "/IntercontinentalExchangeafmig/Manager Hierarchy/Reference"
manage_hierarhy_archive_filepath = "/IntercontinentalExchangeafmig/Manager Hierarchy/Archive"
manage_hierarhy_log_filepath = "/IntercontinentalExchangeafmig/Manager Hierarchy/Logs"

threshold = 400

execution_timeout_days = 14
child_dag_max_active_runs = 2
master_dag_interval = 30
log_filepath = "/IntercontinentalExchangeafmig/Logs"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
tenant_email_for_user_import = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
pacific_timezone = 'US/Pacific'
schedule_interval_daily = '0 22 * * *'

disabled=True
