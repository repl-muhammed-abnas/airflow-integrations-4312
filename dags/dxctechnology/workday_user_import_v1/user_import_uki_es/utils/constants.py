"""
Constants and common utilities for UK&I User Import
"""

# Date format constants
DATE_FORMAT_YYYY_MM_DD = "YYYY-MM-DD"  # Replicon format
DATE_FORMAT_YYYY_DD_MM = "YYYY-DD-MM"  # Workday format
DATE_FORMAT_YYYYMMDD = "YYYYMMDD"
DATE_FORMAT_YYYYMMDD_HHMMSS = "YYYYMMDD_HHmmss"

# Delimiters
LOCATION_DELIMITER = " | "
EMPLOYEE_TYPE_DELIMITER = " | "
PATH_DELIMITER = " | "

# Default values
DEFAULT_TIMEZONE = "GMT Standard Time"
DEFAULT_TIMEZONE_URI = "urn:replicon:timezone:gmt-standard-time"
DEFAULT_WORK_WEEK = "Monday - Sunday"
DEFAULT_SCHEDULE_TYPE = "office"

# URIs
DATA_LOAD_OPTION_URI = "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
USER_STATUS_OPTION_INCLUDE_ALL = "urn:replicon:user-status-option:include-all-users"
PASSWORD_OPTION_USER_SETS = "urn:replicon:password-option:user-sets-password"

# Policies
POLICY_ADMINISTRATION = "urn:replicon:policy:administration"
POLICY_PAYROLL = "urn:replicon:policy:payroll"
POLICY_PROJECT_MANAGEMENT = "urn:replicon:policy:project-management"

# Custom field prefixes
CUSTOM_FIELD_PREFIX = "urn:replicon:custom-field:user:"

# Batch processing
DEFAULT_BATCH_COUNT = 3
DEFAULT_MAX_PARALLEL_TASKS = 5

# Execution timeouts
DEFAULT_EXECUTION_TIMEOUT_DAYS = 1
DEFAULT_EXECUTION_TIMEOUT_HOURS = 1


TIMEOFF_CONFIG = {
    'parttime': {
        'UK': {
            'annual': '[UK] P/T Annual Leave Hrs',
            'bought': '[UK] P/T Bought A/L Hrs',
            'sold': '[UK] P/T Sold A/L Hrs',
            'holiday': '[UK] Public Holiday'
        },
        'IRL': {
            'annual': '[IRL] P/T Annual Leave Hrs',
            'bought': '[IRL] P/T Bought A/L Hrs',
            'sold': '[IRL] P/T Sold A/L Hrs',
            'holiday': '[IRL] Public Holiday'
        },
    },
    'fulltime': {
        'UK': {
            'annual': '[UK] Annual Leave',
            'bought': '[UK] Bought A/L',
            'sold': '[UK] Sold A/L',
        },
        'IRL': {
            'annual': '[IRL] Annual Leave',
            'bought': '[IRL] Bought A/L',
            'sold': '[IRL] Sold A/L',
        },
    },
}
