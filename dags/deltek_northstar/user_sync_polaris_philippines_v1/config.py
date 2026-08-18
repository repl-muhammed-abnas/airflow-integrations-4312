region = 'us-east-1'
environment = 'pre-production'
time_zone = 'US/Eastern'

master_dag_interval = "0 * * * *"
gather_user_logs_timeout_hours = 2

execution_timeout_days = 14
log_file_download_link_expiry_in_sec = 7*24*60*60
max_active_runs_master = 1
max_active_runs_process_supervisor = 2
max_active_runs_process_users = 3
max_active_runs_process_new_users = 3
max_active_runs_process_update_users = 3
max_active_runs_process_log_generation = 1
max_active_runs_process_groups = 3
max_active_runs_process_dropdowns = 3
max_active_runs_process_departments = 3

disable_master_dag_interval = "0 1 * * *"
disable_master_dag_active_runs = 1

trigger_parallel_process_users = 2
trigger_parallel_process_departments = 2

parent_department = 'Deltek Inc'

deltek_cospoint_company_ids = ['PHI']

user_disable_report_name = "User with end date"

valid_personal_action_code = ["HI-NWHIR","HI-INTRN", "HI-ROPTR","HI-ACQUS","HI-REHIR", "HI-REINT","HI-ENTRN","CH-ORGNU","CH-TRANS", "TV-XFR"]
