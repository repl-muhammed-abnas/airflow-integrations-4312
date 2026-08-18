region = 'us-east-1'
environment = 'pre-production'
instance = "trial"
company_key = 'intercontinentalexchangetrial01'
replicon_conn_id = 'intercontinentalexchangetrial01_admin'
can_run_batch_task_var_name = f'IntercontinentalExchange_project_task_import_can_run_batch_task_{instance}'
user_report_name = 'User list - For Integration'
user_report_to_disable = "Enabled User list - For Disabling users"
user_managerhierarchy_report = "Managerhierarchy_Basereport"

sftp_conn_id = "sftp_useast2_internal"
input_filepath = "/Trial/User Demographic Data/Input"
log_filepath = "/Intercontinental/Trial/User Demographic Data/Log"
referance_filepath = "/Intercontinental/Trial/User Demographic Data/Reference"
archive_filepath = "/Intercontinental/Trial/User Demographic Data/Archive"

manage_hierarhy_input_filepath = "/Intercontinental/Trial/Manager Hierarchy/Input"
manage_hierarhy_referance_filepath = "/Intercontinental/Trial/Manager Hierarchy/Reference"
manage_hierarhy_archive_filepath = "/Intercontinental/Trial/Manager Hierarchy/Archive"
manage_hierarhy_log_filepath = "/Intercontinental/Trial/Manager Hierarchy/Log"

triggered_var = 'user_triggered_empids_ice_user_import'

threshold = 400

execution_timeout_days = 14
child_dag_max_active_runs = 2
master_dag_interval = 30
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
tenant_email_for_user_import = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
pacific_timezone = 'US/Pacific'
schedule_interval_daily = '0 22 * * *'
