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

DEFAULT_TIME_OFF_TYPE = "[USA] Vacation"

PAI_EMPLOYEES_LOCATION_LVL_3 = ["FRDRKPAI","DURHAMPAI","DURHAMPAIW","SKOKIEPAI"]

APPLICABLE_TIME_OFF_TYPES = [
    "[USA] Unpaid Absence",
    "[USA] Jury Duty",
    "[USA] Voting Leave",
    "[USA] Bereavement",
    "[USA] Veterans Day",
    "[USA] Facility Closure",
    "[USA] Workers Comp Pay",
    "[USA] Emergency Leave",
    "[USA] Holiday",
    "[USA] Floating Holiday",
    "[USA] Volunteer Day",
    "[USA] Vacation" ,
    "[USA] Sick",
    "[USA] Sick SAL",
    "Holiday"
]

REGULAR_USER_TIME_OFF_TYPES = ["[USA] Jury Duty","[USA] Voting Leave","[USA] Bereavement",
    "[USA] Veterans Day","[USA] Facility Closure","[USA] Workers Comp Pay"]

GLOBAL_TIME_OFF_TYPES = ['[USA] Unpaid Absence']

PLACEHOLDER_BASED_TIMEOFF_TYPES = ["[USA] Vacation", "[USA] Sick", "[USA] Floating Holiday"]

TO_PLACEHOLDER_HIDDEN_OEF_NAMES = ["[USA] Floating Holiday - Placeholder Policy Name",
    "[USA] Vacation - Placeholder Policy Name", "[USA] Sick - Placeholder Policy Name"]

MANNUAL_TIMEOFF_TYPES = []

VP_JOB_CODES_SUFFIX = ["VA","VB","SV","EV","CE","CF","CO","VP"]

SPECIAL_ACCRUAL_TO_TYPES = ["[USA] Vacation", "[USA] Sick"]

pacific_timezone = 'America/Los_Angeles'
report_name = '***Disable User Template - For User Import'
disable_user_master_dag_interval = '0 1 * * *'

sumo_conn_id = 'sumologic-dagrunlogger'

END_DATE_STATUS = ['Terminated','Retired','Discarted','Deceased']

