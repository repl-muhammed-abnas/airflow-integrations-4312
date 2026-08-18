region = 'us-east-2'
environment = 'pre-production'

master_dag_interval = 30
execution_timeout_days = 14

master_dag_active_runs = 1
child_dag_groups_max_active_runs = 1
child_dag_udfs_max_active_runs = 1
child_dag_disableuser_max_active_runs = 30
child_dag_adduser_max_active_runs = 30
child_dag_updateuser_max_active_runs = 30
child_dag_timeoff_assignment_max_active_runs = 5
child_dag_referencefile = 5
child_dag_supervisor_assignment = 10

dag_max_active_tasks = 200

input_filepath = '/ftp/10000841'
archive_filepath = '/ftp/10000841/archive'
reference_filepath = '/ftp/10000841/reference'
log_filepath = '/ftp/10000841/logs'
sumo_conn_id = 'sumologic-dagrunlogger'

disable_user_reportname = '**Userlist for disabling User'
disable_user_threshold = 300

use_reference_file = True
