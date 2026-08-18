region = 'eu-central-1'
environment = 'pre-production'

execution_timeout_days = 14

dag_max_active_tasks = 10000
# use 20 concurrency for production - refer project_import_api_v1
master_dag_max_active_runs = 5
child_dag_process_project_max_active_runs = 5
child_dag_create_project_max_active_runs = 5
child_dag_update_project_max_active_runs = 5
#--------------

child_dag_log_generation_max_active_runs = 5 # use 10 for production
master_scheduled_log_generation_max_active_runs = 1
child_dag_scheduled_log_generation_max_active_runs = 5

project_import_log_name = "project_import_final_logs_trial"
log_filepath = "/PwCGlobal/Project_Import/logs"

dagrun_log_conn_id = 'sumologic-dagrunlogger'
log_generation_dag_interval = '0 * * * *'
# Need to be updated based on log_generation_dag_interval
lookup_log_timestamp_hours = 1

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# copy the below lines to the respective instance config if secondary sftp needs to be set up
secondary_sftp = False  # set this to True to enable secondary_sftp
if secondary_sftp:
    secondary_sftp_conn_id = 'sftp_pwc_projectimport'
    secondary_log_filepath = "/PwCGlobal/Project_Import/logs"
