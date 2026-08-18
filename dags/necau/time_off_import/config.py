region = 'eu-central-1'
environment = 'pre-production'
company_key = 'NECAUafmig'
replicon_conn_id = 'NECAUafmig_replicon_admin'
user_shift_report_name = "***Auto Shift Assignment-Master***"
timeoff_import_user_referance = "***Timeoff Import User Reference"
dag_max_active_runs = 10
dag_max_active_tasks = 128  # Please remove it
execution_timeout_days = 14
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

processing_file_directory = "/NECAU/timeofafmig/FromFrontier/processing"
unprocessed_file_directory = "/NECAU/timeofafmig/FromFrontier/unprocessed"
timeoff_import_file_directory = "/NECAU/timeofafmig/FromFrontier"
archive_file_directory = "/NECAU/timeofafmig/FromFrontier/archived"

alert_email = '{{ var.value.dagrun_internal_testing_email }}'
schedule_interval_daily = '0 * * * *'
master_dag_active_runs = 1
child_dag_active_runs = 1
time_zone = 'Australia/Melbourne'
