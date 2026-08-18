# pylint: disable=wildcard-import unused-wildcard-import invalid-name
"""
CRL User Import - Israel Country Configuration (Layer 2)

Overrides config_base.py defaults with Israel-specific values.
Instance files import from here: from crl.user_import_generic_row.countries.israel.country_config import *
"""
from datetime import datetime, timedelta
from crl.user_import_generic_row.config_base import *  # noqa: F401,F403

region = "us-east-1"
environment = "pre-production"

# ---------------------------------------------------------------------------
# Country Identity
# ---------------------------------------------------------------------------
COUNTRY_CODE = "ISR"
COUNTRY_NAME = "Israel"
DAG_PREFIX = "crl_user_import_israel"
TIME_ZONE = "Asia/Jerusalem"

# ---------------------------------------------------------------------------
# Concurrency Overrides (small user base ~2 users)
# ---------------------------------------------------------------------------
max_active_runs_process_users = 5
max_active_runs_process_new_users = 3
max_active_runs_process_update_users = 3
max_active_runs_process_disable_users = 3
max_active_runs_process_supervisor = 5
trigger_parallel_dagrun_count_process_users = 5

# ---------------------------------------------------------------------------
# Time-Off Types
# ---------------------------------------------------------------------------
DEFAULT_TIME_OFF_TYPE = "[ISR] Vacation Leave (Annual Paid Leave)"

APPLICABLE_TIME_OFF_TYPES = [
    "[ISR] Vacation Leave (Annual Paid Leave)",
    "[ISR] Sickness (Short Term)",
    "[ISR] Medical Appointment Leave",
    "[ISR] Child Sickness (Short Term)",
    "[ISR] Bereavement Leave",
    "[ISR] Short Military Reserve Duty Leave",
    "[ISR] Short Unpaid Leave",
    "Holiday"
]

GLOBAL_TIME_OFF_TYPES = [
    "[ISR] Vacation Leave (Annual Paid Leave)",
    "[ISR] Sickness (Short Term)",
    "[ISR] Medical Appointment Leave",
    "[ISR] Child Sickness (Short Term)",
    "[ISR] Bereavement Leave",
    "[ISR] Short Military Reserve Duty Leave",
    "[ISR] Short Unpaid Leave",
    "Holiday"
]

MANNUAL_TIMEOFF_TYPES = []

# ---------------------------------------------------------------------------
# Mapper Configuration Overrides
#
# Israel uses a simple mapper: single row matching All/All/All.
# Fewer matching keys needed compared to UK.
# ---------------------------------------------------------------------------
MAPPER_KEYS_FOR_DATA_RETRIEVE = [
    'location_level_1', 'location_level_2', 'location_level_3',
    'company_code', 'reg_temp', 'pay_type', 'full_part', 'activity_type'
]

KEY_MAPPING_FOR_FEED_FIELDS = {
    'location_level_1': 'location_full_path',
    'location_level_2': 'location_full_path',
    'location_level_3': 'location_full_path',
    'company_code': 'company_code',
    'reg_temp': 'reg_temp',
    'pay_type': 'pay_type',
    'full_part': 'full_part',
    'activity_type': 'activity_type'
}

SHOULD_CHECK_FOR_ALL_EXCEPT = [
    'location_level_2', 'location_level_3', 'activity_type'
]

# ---------------------------------------------------------------------------
# Israel-Specific Defaults
# ---------------------------------------------------------------------------
DEFAULT_SCHEDULE_NAME = "S140501"
DEFAULT_WORK_WEEK = "Sunday - Saturday"
DEFAULT_LANGUAGE = "English"

# ---------------------------------------------------------------------------
# Report-Based Daily Disable
# ---------------------------------------------------------------------------
ENABLE_REPORT_BASED_DISABLE = False

# ---------------------------------------------------------------------------
# Hook Functions — Israel-Specific Overrides
# ---------------------------------------------------------------------------


def get_payrule_effective_date_hook(change_effective_date, date_format="%m/%d/%Y"):
    """Israel: following Sunday from change_effective_date."""
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


def get_holiday_calendar_hook(item, matched_mapper_row):
    """Israel: holiday calendar from mapper row."""
    return matched_mapper_row.get('holiday_calendar', 'ISR_Holiday Calendar')


def get_default_schedule_hook():
    """Israel: default schedule S140501."""
    return DEFAULT_SCHEDULE_NAME
