region = "us-east-1"
environment = "pre-production"

master_dag_interval = 30
file_sensor_timeout = 10
execution_timeout_days = 14
gather_user_logs_timeout_hours = 2


max_active_runs_master = 1
max_active_runs_process_groups = 1
max_active_runs_process_locations = 1
max_active_runs_process_costcenters = 1
max_active_runs_process_departments = 1
max_active_runs_process_divisions = 1
max_active_runs_process_servicecenters = 1
max_active_runs_process_log_generation = 1

max_active_runs_process_users = 10
max_active_runs_process_new_users = 10
max_active_runs_process_update_users = 10

trigger_parallel_dagrun_count_process_users = 10
trigger_parallel_dagrun_count_process_locations = 1
trigger_parallel_dagrun_count_process_departments = 1
trigger_parallel_dagrun_count_process_servicecenters = 1
trigger_parallel_dagrun_count_process_costcenters = 1
trigger_parallel_dagrun_count_process_divisions = 1

WORKWEEK = "urn:replicon:day-of-week:saturday"
TIMESHEETPERIOD = "Weekly starting on Sunday"

DEFAULT_CONTRACTOR_SERVICE_CENTER_NAME = "SubContractors"
DEFAULT_CONTRACTOR_SERVICE_CENTER_CODE = "1710"
