region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'

max_active_run_master = 1
max_active_run_create_job_title_child = 3
max_active_run_create_office_schedules_child = 3
max_active_run_create_custom_fields_child = 3
max_active_run_update_user_child = 3
max_active_run_create_user_supervisor_child = 1
max_active_run_create_user_child = 1
max_active_run_process_users_child = 5

trigger_parallel_dagrun_count_create_job_title_child = 3
trigger_parallel_dagrun_count_create_office_schedules_child = 3
trigger_parallel_dagrun_count_process_users = 5

# Timeouts
execution_timeout_days = 14
file_sensor_timeout = 10
responses_from_child_timeout = 10

# Schedule
schedule_interval = "5 0 * * *"

timezone = "America/Chicago"


# Input file field definitions: (field_name, is_mandatory)
# Order matters — it defines the column order in the input/CSV files.
FIELD_DEFINITIONS = (
    ("empl_id",                True),
    ("email_id",               True),
    ("last_name",              True),
    ("first_name",             True),
    ("hire_or_rehire",         True),
    ("term_date",              False),
    ("location_description",   False),
    ("company_name",           True),
    ("location_state",         False),
    ("country",                True),
    ("payroll_dept_no",        True),
    ("payroll_dept_name",      True),
    ("rpc",                    False),
    ("job_code",               False),
    ("job_title",              True),
    ("standard_hours",         True),
    ("hrly_or_salary",         True),
    ("reports_to_manager_id",  False),
    ("executive_level",        True),
    ("report_to_name",         True),
    ("empl_status",            False),
)

INPUT_FILE_HEADERS = tuple(field for field, _ in FIELD_DEFINITIONS)
CSV_FILE_HEADERS = (*INPUT_FILE_HEADERS, "md5")
MANDATORY_FIELDS = frozenset(field for field, required in FIELD_DEFINITIONS if required)

REPLICON_USER_LIST_REPORT_NAME = "Enabled users list - For Integration"
REPLICON_USER_LIST_REPORT_EXPECTED_COLUMNS = "User Name,User Email,Employee ID,UserUri"
REPLICON_USER_LIST_COLUMNS = ("user_name", "user_email", "employee_id", "user_uri")

# Table names
USER_INPUT_TABLE = "user_input_data"