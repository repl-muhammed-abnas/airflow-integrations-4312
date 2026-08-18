# pylint: disable=invalid-name
"""
CRL Generic User Import - Base Configuration (Layer 1)

Universal defaults shared across ALL countries. Country-specific configs
override these values via wildcard import:
    from crl.user_import_generic_row.config_base import *

Architecture:
    config_base.py  →  countries/{country}/config.py  →  instances/{country}_{env}.py
    (Layer 1)           (Layer 2: overrides)               (Layer 3: env specifics)
"""
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Region / Environment (overridden by instance files)
# ---------------------------------------------------------------------------
region = "us-east-1"
environment = "pre-production"

# ---------------------------------------------------------------------------
# Country Identity (MUST be overridden by country config)
# ---------------------------------------------------------------------------
COUNTRY_CODE = None          # e.g. "ISR", "CHE"
COUNTRY_NAME = None          # e.g. "Israel", "Switzerland"
DAG_PREFIX = None            # e.g. "crl_user_import_israel"
TIME_ZONE = "UTC"            # pytz timezone, e.g. "Asia/Jerusalem", "Europe/Zurich"

# ---------------------------------------------------------------------------
# Timeouts
# ---------------------------------------------------------------------------
execution_timeout_days = 14
gather_user_logs_timeout_hours = 12

# ---------------------------------------------------------------------------
# Concurrency — DAG max_active_runs
# ---------------------------------------------------------------------------
max_active_runs_process_user_import_payload = 1
max_active_runs_process_groups = 1
max_active_runs_process_buisness_unit = 1
max_active_runs_process_company_code = 1
max_active_runs_process_cost_center = 1
max_active_runs_process_location = 1
max_active_runs_process_new_departments = 1

max_active_runs_process_users = 20
max_active_runs_process_new_users = 10
max_active_runs_process_update_users = 10
max_active_runs_process_disable_users = 10
max_active_runs_process_supervisor = 20
max_active_runs_process_log_generation = 1

max_active_runs_process_timeoff_type_no_accrual = 20
max_active_runs_process_time_off_type_assignment_new_user = 10
max_active_runs_process_time_off_type_assignment_update_rehire_user = 10

disable_user_master_dag_active_runs = 1
disable_user_child_dag_active_runs = 2

trigger_parallel_dagrun_count_process_users = 15

# ---------------------------------------------------------------------------
# Status Classifications (identical across ALL existing CRL countries)
# ---------------------------------------------------------------------------
ACTIVE_STATUS = ['Active', 'Paid Leave', 'Furlough', 'Dormant']
DISABLE_STATUS = ['Terminated', 'Unpaid Leave', 'Suspended', 'Retired', 'Discarted', 'Deceased']
END_DATE_STATUS = ['Terminated', 'Retired', 'Discarted', 'Deceased']
IGNORE_STATUS_ZERO_ACCRUAL = ['Unpaid Leave', 'Suspended']

ALL_VALID_STATUSES = ACTIVE_STATUS + DISABLE_STATUS

# ---------------------------------------------------------------------------
# Batch Processing
# ---------------------------------------------------------------------------
BATCH_COUNT = 3

# ---------------------------------------------------------------------------
# Time-Off Configuration (MUST be overridden by country config)
# ---------------------------------------------------------------------------
DEFAULT_TIME_OFF_TYPE = None
APPLICABLE_TIME_OFF_TYPES = []
GLOBAL_TIME_OFF_TYPES = []
MANNUAL_TIMEOFF_TYPES = []

# ---------------------------------------------------------------------------
# Mapper Configuration (MUST be overridden by country config)
# ---------------------------------------------------------------------------
MANDATORY_FIELDS = {
    "emp_id": "Empl_ID",
    "first_name": "First_Name",
    "last_name": "Last_Name",
    "email": "Work_Email",
    "login_name": "User_Name",
    "emp_status": "Empl_Status",
    "buisness_unit_full_path": "Bus_Seg_Unit",
    "company_code": "Company",
    "location_full_path": "Location",
    "reg_temp": "Reg_Temp",
    "full_part": "Full_Part",
    "start_date": "Hire_Date",
    "adjusted_hire_date": "Adj_Hire_Date",
    "job_code": "Job_Code",
    "pay_type": "Pay_Type",
    "cost_center_full_path": "Cost_Center_Business_Area",
}

KEY_MAPPING_FOR_FEED_FIELDS = {
    'location_level_2': 'location_full_path',
    'location_level_3': 'location_full_path',
    'company_code': 'company_code',
    'buisness_unit_level_2': 'buisness_unit_full_path',
    'buisness_unit_level_1': 'buisness_unit_full_path',
    'functional_segment': 'functional_segment',
    'functional_sub_segment': 'functional_sub_segment',
    'cost_center': 'cost_center_full_path',
    'department': 'department',
    'job_code': 'job_code',
    'job_level': 'job_level',
    'reg_temp': 'reg_temp',
    'pay_type': 'pay_type',
    'full_part': 'full_part',
    'pay_scale_group': 'pay_scale_group',
    'activity_type': 'activity_type'
}

MAPPER_KEYS_FOR_DATA_RETRIEVE = [
    'location_level_2', 'location_level_3', 'company_code',
    'buisness_unit_level_2', 'reg_temp', 'buisness_unit_level_1',
    'functional_segment', 'functional_sub_segment', 'cost_center',
    'department', 'job_code', 'job_level', 'full_part', 'pay_type',
    'pay_scale_group', 'activity_type'
]

SHOULD_CHECK_FOR_ALL_EXCEPT = [
    'location_level_3', 'buisness_unit_level_2',
    'job_code', 'job_level', 'pay_scale_group'
]

# ---------------------------------------------------------------------------
# Report-Based Daily Disable (optional feature)
# ---------------------------------------------------------------------------
ENABLE_REPORT_BASED_DISABLE = False
DISABLE_REPORT_NAME = None
DISABLE_SCHEDULE_INTERVAL = None

# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------
sumo_conn_id = 'sumologic-dagrunlogger'

# ---------------------------------------------------------------------------
# Hook Functions — Strategy Pattern
#
# Country configs override these to customize behaviour.
# Generic utils call config.hook_name() so the right country logic runs.
# ---------------------------------------------------------------------------


def get_payrule_effective_date_hook(change_effective_date, date_format="%m/%d/%Y"):
    """Default: following Sunday from change_effective_date."""
    if not change_effective_date:
        return None
    date = datetime.strptime(change_effective_date, date_format)
    days_ahead = 6 - date.weekday()  # Sunday = 6
    if days_ahead <= 0:
        days_ahead += 7
    next_sunday = date + timedelta(days=days_ahead)
    return {
        'year': next_sunday.year,
        'month': next_sunday.month,
        'day': next_sunday.day
    }


def get_overtime_eligibility_hook(item):
    """Default: no overtime eligibility logic (returns None)."""
    return None


def get_default_time_off_type_hook():
    """Default: returns DEFAULT_TIME_OFF_TYPE from config."""
    return DEFAULT_TIME_OFF_TYPE


def get_holiday_calendar_hook(item, matched_mapper_row):
    """Default: use holiday_calendar from the matched mapper row."""
    return matched_mapper_row.get('holiday_calendar', '')


def get_default_schedule_hook():
    """Default: returns None (country must override if needed)."""
    return None


def get_time_off_types_hook(dag_run, applicable_types, global_types):
    """Default: assign only GLOBAL_TIME_OFF_TYPES (those in both lists).
    Countries with employee-type-specific time-off types (e.g. Switzerland)
    override this hook to add extra types based on dag_run.conf['employee_type_name'].
    """
    return [
        {"actual_timeoff_type_name": t, "placeholder_timeoff_type_name": "NA"}
        for t in applicable_types if t in global_types
    ]


def post_process_user_payload_hook(payload, item, config_module):
    """Default: no-op. Countries can add extra fields/transforms."""
    return payload
