# pylint: disable=wildcard-import unused-wildcard-import invalid-name
"""
CRL User Import - Switzerland Country Configuration (Layer 2)

Overrides config_base.py defaults with Switzerland-specific values.
Instance files import from here: from crl.user_import_generic_row.countries.switzerland.country_config import *

Key Switzerland differences from Israel:
- Two mapper rows split by Functional Segment (IS vs All Except IS)
- Holiday calendar driven by holidayCalendarCode from feed (canton-specific)
- OT Eligible (IS) gets 8 time-off types; Non Eligible gets 7
- Work week: Monday to Sunday (vs Israel's Sunday to Saturday)
- Payrule effective date: following Monday (vs Israel's following Sunday)
"""
from datetime import datetime, timedelta
from crl.user_import_generic_row.config_base import *  # noqa: F401,F403

# ---------------------------------------------------------------------------
# Country Identity
# ---------------------------------------------------------------------------
COUNTRY_CODE = "CHE"
COUNTRY_NAME = "Switzerland"
DAG_PREFIX = "crl_user_import_switzerland"
TIME_ZONE = "Europe/Zurich"

# ---------------------------------------------------------------------------
# Time-Off Types
# ---------------------------------------------------------------------------
DEFAULT_TIME_OFF_TYPE = "[CHE] Vacation"

# All 8 types that may ever be assigned to any Switzerland employee
APPLICABLE_TIME_OFF_TYPES = [
    "[CHE] Vacation",
    "[CHE] Sickness (Short-Term)",
    "[CHE] Family Leave",
    "[CHE] Breastfeeding Breaks",
    "[CHE] Unpaid Vacation",
    "[CHE] Flex Day",
    "[CHE] Volunteer Time Off (VTO)",
    "[CHE] Time off in Lieu 1.0",
    "[CHE] Time off in Lieu 1.25",
    "[CHE] Time off in Lieu 1.5",
    "Holiday"
]

# 7 types assigned to ALL Switzerland employees (both OT Eligible and OT Not Eligible)
GLOBAL_TIME_OFF_TYPES = [
    "[CHE] Vacation",
    "[CHE] Sickness (Short-Term)",
    "[CHE] Family Leave",
    "[CHE] Breastfeeding Breaks",
    "[CHE] Unpaid Vacation",
    "[CHE] Flex Day",
    "[CHE] Volunteer Time Off (VTO)",
    "Holiday"
]

# 1 additional type assigned ONLY to CHE_OT Eligible employees (IS functional segment)
OT_ELIGIBLE_ONLY_TIME_OFF_TYPES = [
    "[CHE] Time off in Lieu 1.0",
    "[CHE] Time off in Lieu 1.25",
    "[CHE] Time off in Lieu 1.5"
]

MANNUAL_TIMEOFF_TYPES = []

# ---------------------------------------------------------------------------
# Mapper Configuration Overrides
#
# Switzerland needs functional_segment as a matcher key to distinguish
# IS (OT Eligible) from all other segments (OT Not Eligible).
# functional_segment uses "All Except IS" pattern for row 2.
# ---------------------------------------------------------------------------
MAPPER_KEYS_FOR_DATA_RETRIEVE = [
    'location_level_1', 'location_level_2', 'location_level_3',
    'company_code', 'reg_temp', 'pay_type', 'full_part', 'activity_type',
    'functional_segment',
]

KEY_MAPPING_FOR_FEED_FIELDS = {
    'location_level_1': 'location_full_path',
    'location_level_2': 'location_full_path',
    'location_level_3': 'location_full_path',
    'company_code': 'company_code',
    'reg_temp': 'reg_temp',
    'pay_type': 'pay_type',
    'full_part': 'full_part',
    'activity_type': 'activity_type',
    'functional_segment': 'functional_segment',
}

# functional_segment supports "All Except IS" pattern for row 2
SHOULD_CHECK_FOR_ALL_EXCEPT = [
    'location_level_2', 'location_level_3', 'activity_type', 'functional_segment',
]

# ---------------------------------------------------------------------------
# Report-Based Daily Disable
# ---------------------------------------------------------------------------
ENABLE_REPORT_BASED_DISABLE = False

# ---------------------------------------------------------------------------
# Hook Functions — Switzerland-Specific Overrides
# ---------------------------------------------------------------------------

def get_payrule_effective_date_hook(change_effective_date, date_format="%m/%d/%Y"):
    """Switzerland: following Monday from change_effective_date.
    Timesheet period is CHE - Weekly starting on Monday.
    """
    if not change_effective_date:
        return None
    date = datetime.strptime(change_effective_date, date_format)
    days_ahead = 0 - date.weekday()  # Monday = 0
    if days_ahead <= 0:
        days_ahead += 7
    next_monday = date + timedelta(days=days_ahead)
    return {
        'year': next_monday.year,
        'month': next_monday.month,
        'day': next_monday.day,
    }


def get_holiday_calendar_hook(item, matched_mapper_row):
    """Switzerland: holiday calendar is canton-specific, driven by holidayCalendarCode
    field from the SuccessFactors feed (mapped to item['holiday_calendar']).
    The mapper row contains '' as a placeholder — always use the feed value.
    Known calendars: CHE_ZH (Zurich), CHE_BS (Basel-Stadt), CHE_TI (Ticino), etc.
    """
    return item.get('holiday_calendar', '')


def get_time_off_types_hook(dag_run, applicable_types, global_types):
    """Switzerland: all employees get 7 global types. OT Eligible (CHE_OT Eligible)
    additionally receives 1 type: Time off in Lieu (8 total).
    employee_type_name is set in dag_run.conf from the mapper lookup.
    """
    base = [
        {"actual_timeoff_type_name": t, "placeholder_timeoff_type_name": "NA"}
        for t in applicable_types if t in global_types
    ]
    if dag_run.conf.get('employee_type_name', '') == 'CHE_OT Eligible':
        base += [
            {"actual_timeoff_type_name": t, "placeholder_timeoff_type_name": "NA"}
            for t in OT_ELIGIBLE_ONLY_TIME_OFF_TYPES
        ]
    return base


def get_default_schedule_hook():
    """Switzerland: CHES140 is the fallback schedule when the SF payload schedule
    is absent or does not exist in Replicon. Spec §7: 'In case the schedule is not
    available in Replicon, assign a default schedule (CHES140).'
    """
    return "CHES140"
