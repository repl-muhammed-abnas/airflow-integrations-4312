region = 'us-east-1'
environment = "pre-production"

# Execution settings
child_max_active_run = 5
USERS_COUNT = 3
BATCHES_PER_USER = 5
TOTAL_BATCHES = USERS_COUNT * BATCHES_PER_USER
execution_timeout_hours = 24
execution_timeout_days = 14

# Date validation settings (in days)
ENTRY_DATE_MIN_DAYS_PAST = 28
ENTRY_DATE_MAX_DAYS_FUTURE = 3

# Timesheet template name keywords (matched case-insensitively against template name in Replicon)
TIMESHEET_TEMPLATES = {
    'TSD': ['TSD', 'TDG', 'Agile Standard TDG'],
    'In/Out': ['In/Out'],
    'Punch': ['Punch In/Punch Out', 'Punch In', 'Punch'],
}
MANDATORY_OEFS_ALL = [
    'Work Performed Location',
    'Year Work Relates To',
]
MANDATORY_OEFS_BY_TEMPLATE_PREFIX = {
    'Germany': ['Work Location'],
    'Romania': ['Work Location Type'],
}

# Customer endpoint for posting logs
customer_log_endpoint_conn_id = "vialto_log_endpoint"
