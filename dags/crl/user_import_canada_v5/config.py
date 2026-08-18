region = 'us-east-1'
environment = "pre-production"

max_active_run_wehook_master = 1
execution_timeout_days = 14
gather_user_logs_timeout_hours = 12

max_active_runs_process_user_import_payload = 1
max_active_runs_process_groups = 1
max_active_runs_process_buisness_unit = 1
max_active_runs_process_company_code = 1
max_active_runs_process_cost_center = 1
max_active_runs_process_location = 1

max_active_runs_process_users = 10
max_active_runs_process_new_users = 5
max_active_runs_process_update_users = 5
max_active_runs_process_disable_users = 5
max_active_runs_process_supervisor = 5
max_active_runs_process_log_generation = 1

max_active_runs_process_timeoff_type_no_accrual = 5
max_active_runs_process_montreal_vacation_new_user = 5
max_active_runs_process_time_off_type_assignment_new_user = 5
max_active_runs_process_time_off_type_assignment_update_rehire_user = 5

disable_user_master_dag_active_runs = 1
disable_user_child_dag_active_runs = 5

trigger_parallel_dagrun_count_process_users = 10

CANADA_LANGUAGE_URI = "urn:replicon:language:fr-FR"

pacific_timezone = 'America/Los_Angeles'
report_name = '***Disable User Template - For User Import'
disable_user_master_dag_interval = '0 1 * * *'

sumo_conn_id = 'sumologic-dagrunlogger'

ACTIVE_STATUS = ['Active','Paid Leave','Furlough','Dormant']
DISABLE_STATUS = ['Terminated','Unpaid Leave','Suspended','Retired','Discarted','Deceased']

BATCH_COUNT = 3

IGNORE_STATUS_ZERO_ACCRUAL = ['Suspended']

FULL_TIME_HRS_VACATION = 37.5

END_DATE_STATUS = ['Terminated','Retired','Discarted','Deceased']
