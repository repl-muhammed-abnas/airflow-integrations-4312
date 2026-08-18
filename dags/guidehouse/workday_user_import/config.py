# Company Information
region = "us-east-1"
environment = "pre-production"

# Execution Settings
execution_timeout_days = 14
master_dag_interval = 30
file_sensor_timeout = 10

max_active_run_master = 1
max_active_runs_process_users = 10
max_active_runs_process_new_users = 10
max_active_runs_process_update_users = 10
max_active_runs_process_supervisor = 10
max_active_runs_process_log_generation = 1
max_active_runs_disable_profile_master = 1
max_active_runs_process_divisions = 10
schedule_dag_max_active_runs = 10
max_active_runs_process_schedule = 10
max_active_runs_process_zero_timeoff_policies = 10

trigger_parallel_dagrun_count_process_users = 20
trigger_parallel_dagrun_count_process_locations = 2
trigger_parallel_dagrun_count_process_usertypes = 2
trigger_parallel_dagrun_count_process_schedules = 2

PROCESS_USER_BATCH_COUNT = 10

log_file_download_link_expiry_in_sec = 7 * 24 * 60 * 60

disable_master_dag_interval = "0 1 * * *"
disable_master_dag_active_runs = 1

gather_user_logs_timeout_hours = 24

user_disable_report_name = "User with end date"

# Default Values
default_language = 'en'

# License Types
licenses = ["TOE", "WFM", "Polaris PSA"]

# Input file header (23 columns)
input_file_header = 'Employee_ID,Login_Name,First_Name,Last_Name,Email,Supervisor_ID,Default_Location,Employee_Type,Change_Effective_Date,Schedule,Start_Date,Seniority_Date,End_Date,Job_code,Job_Description,Pay_Group,Status,Company_Code,Company_Description,Cost_Center_Code,Cost_Center_Description,Financial_System,Time_Profile_Name'

# Custom UDF fields for Guidehouse
CUSTOM_FIELDS = ['Change Effective Date', 'Seniority Date', 'Job Code', 'Job Description', 'Pay Group', 'Status', 'Time Profile Name']

# Holiday calendar logic:
# Germany and India use Location Level 3 (city/state level)
# All other countries use Location Level 1 (country level)
LEVEL3_HOLIDAY_CALENDAR_FOR = ['india', 'germany']

# Default activity logic:
# USA and Canada Non-Exempt employees use Location Level 2 as default activity
# All others use Location Level 1
LEVEL2_DEFAULT_ACTIVITY_COUNTRIES = ['united states of america', 'canada']
NON_EXEMPT_EMPLOYEE_TYPE_LEVEL2 = 'Non-Exempt'

# Timezone defaults for multi-timezone countries
MULTI_TIMEZONE_DEFAULT = 'Eastern Standard Time'
MULTI_TIMEZONE_COUNTRIES = ['usa', 'united states of america', 'canada']

# India uses Semi-Monthly timesheet period; all others use Weekly
INDIA_TIMESHEET_PERIOD = 'Semi-Monthly'

# No out-of-scope locations for Guidehouse
OUT_OF_SCOPE_LOCATIONS = []

# Permission sets required for Guidehouse users
# TODO: Populate with actual Guidehouse Replicon permission set names
PERMISSIONS = []

# Time-off types excluded from LOA (Leave of Absence) processing.
# These types are never disabled or zeroed out during schedule changes or terminations.
LOA_EXCLUDED_TIMEOFF_TYPES = [
    "Administrative Leave",
    "Caregiver Leave",
    "FMLA Sick",
    "Long Term Disability",
    "Military",
    "ONLTIL DTO",
    "ONLTIL Sick",
    "ONLTIL STD",
    "ONLTIL Unpaid",
    "Unpaid Administrative Leave",
    "Unpaid Leave",
    "EMEC Only - Paternity Leave",
    "EMEC Only - Family Leave",
    "EMEC Only - Adoption Leave",
    "EME Only - Compassion Leave",
    "EMEC RTT Leave",
    "EMEC Citizenship Leave",
    "EME UNPAID LEAVE - LTD",
    "EME UNPAID LEAVE - Family Leave",
    "EME UNPAID LEAVE - Personal Leave",
    "EME UNPAID LEAVE \u2013 Admin",
    "EMEC Segment Only Short Term Disability",
    "EMEC Segment Only Long Term Disability",
]