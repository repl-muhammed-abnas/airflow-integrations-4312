region = "us-east-1"

environment = "pre-production"

execution_timeout_days = 14
gather_user_logs_timeout_hours = 12

max_active_runs_process_user_import_payload = 1
max_active_runs_process_groups = 1
max_active_runs_process_buisness_unit = 1
max_active_runs_process_company_code = 1
max_active_runs_process_cost_center = 1
max_active_runs_process_location = 1
max_active_runs_process_new_departments = 1

max_active_runs_process_users = 20
max_active_runs_process_new_users = 20
max_active_runs_process_update_users = 20
max_active_runs_process_disable_users = 20
max_active_runs_process_supervisor = 20
max_active_runs_process_log_generation = 1

max_active_runs_process_timeoff_type_no_accrual = 20
max_active_runs_process_vacation_new_user = 20
max_active_runs_process_time_off_type_assignment_new_user = 20
max_active_runs_process_time_off_type_assignment_update_rehire_user = 20

disable_user_master_dag_active_runs = 1
disable_user_child_dag_active_runs = 2

trigger_parallel_dagrun_count_process_users = 15

ACTIVE_STATUS = ['Active','Paid Leave','Furlough','Dormant']
DISABLE_STATUS = ['Terminated','Unpaid Leave','Suspended','Retired','Discarted','Deceased']

BATCH_COUNT = 3

IGNORE_STATUS_ZERO_ACCRUAL = ['Unpaid Leave', 'Suspended']

DEFAULT_TIME_OFF_TYPE = "[IRE] Annual Leave"

MANNUAL_TIMEOFF_TYPES = []


sumo_conn_id = 'sumologic-dagrunlogger'

END_DATE_STATUS = ['Terminated','Retired','Discarted','Deceased']

