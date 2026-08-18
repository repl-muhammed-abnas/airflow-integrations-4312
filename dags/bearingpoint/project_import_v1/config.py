region = 'eu-central-1'
environment = "pre-production"

dagrun_log_conn_id = 'sumologic-dagrunlogger'

user_report_name = '***User Base Report'
expected_user_report_columns = 'Employee ID,UserUri,Permission Name'

permission_sets = {
    'project_manager': 'Project Manager',
    'project_comanager': 'Co Manager',
    'client_manager': 'Client Representative'
}

master_max_active_run = 1
max_active_runs_child=5
execution_timeout_days=14
parallel_count =10
