region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14
gather_user_logs_timeout_hours = 2

time_zone = "US/Pacific"

master_dag_active_runs = 1
management_level_child_dag_active_runs = 5
add_user_child_dag_active_runs = 5
update_user_child_dag_active_runs = 5
assign_supervisor_child_dag_active_runs = 5
process_eachuser_child_dag_active_runs = 5
process_customfield_dd_child_dag_active_runs = 5
disable_user_child_dag_active_runs = 5
assign_sub_user_child_dag_active_runs = 5
assign_balance_timeoff_child_dag_active_runs = 5
process_each_user_parallel_dagruns_count = 25
