from datetime import timedelta
region = 'eu-central-1'
environment = 'pre-production'

execution_timeout_days = 14

dag_max_active_tasks = 120
master_dag_max_active_runs = 1
child_dag_process_project_max_active_runs = 5
child_dag_create_project_max_active_runs = 5
child_dag_update_project_max_active_runs = 5
child_dag_log_generation_max_active_runs = 2

dagrun_log_conn_id = 'sumologic-dagrunlogger'

should_archive = False

# copy the below lines to the respective instance config if secondary sftp needs to be set up
secondary_sftp = False  # set this to True to enable secondary_sftp
if secondary_sftp:
    secondary_sftp_conn_id = 'sftp_pwc_projectimport'
    secondary_log_filepath = "/PwCGlobal/Project_Import_FileBased/logs"

schedule_interval = timedelta(seconds=30)
