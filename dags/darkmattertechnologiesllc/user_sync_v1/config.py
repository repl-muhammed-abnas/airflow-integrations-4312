region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14
gather_user_logs_timeout_hours = 2

user_disable_report_name = "***User Disable Template Report ***"
ref_file_name = "UserImport_Reference.csv"

master_dag_interval = 30
decryption_schedule_interval = 30

master_dag_active_runs = 1
add_user_child_dag_active_runs = 1
update_user_child_dag_active_runs = 1
assign_supervisor_child_dag_active_runs = 1
process_eachuser_child_dag_active_runs = 1
assign_timeoff_newuser_child_dag_active_runs = 1
max_active_process_run_count = 2

decryption_dag_active_runs = 1

disable_master_dag_interval = "0 1 * * *"
disable_master_dag_active_runs = 1

max_active_run_groups_child = 1

max_active_runs_process_log_generation = 1

time_zone = 'EST'

schedule_interval = "0 5 * * *"
