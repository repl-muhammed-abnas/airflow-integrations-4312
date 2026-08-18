"""
GuestTek Talent to Replicon User Import Integration - Configuration Module

This module contains shared configuration values for all DAGs and instances
in the GuestTek Talent User Import integration.

Configuration Categories:
    - Execution Settings: Timeouts, intervals, and retry configurations
    - Max Active Runs: Concurrent execution limits for each DAG
    - Parallel Processing: Parallel dagrun counts for batch operations
    - Default Values: Language and timezone defaults
    - License Types: Available Replicon license types
    - Delta Detection: User modification time window
    - Custom Fields: OEF field definitions

Constants:
    execution_timeout_days (int): Maximum DAG execution timeout (14 days)
    DELTA_HOURS (int): Hours to look back for user modifications (24)
"""

# Company Information
region = "us-east-1"
environment = "pre-production"

# Execution Settings
execution_timeout_days = 14
master_dag_interval = "15 0 * * *"  # Daily at 12:15 AM MST (cron format)

# Max Active Runs
max_active_run_master = 1
max_active_runs_process_groups = 4
max_active_runs_process_users = 5
max_active_runs_process_new_users = 5
max_active_runs_process_update_users = 5
max_active_runs_process_supervisor = 5
max_active_runs_process_employeetype = 4
max_active_runs_process_log_generation = 1
max_active_runs_process_roles = 4
max_active_runs_process_role = 4
max_active_runs_process_service_centers = 4
max_active_runs_process_service_center = 4

# Parallel Processing Counts
trigger_parallel_dagrun_count_process_usertypes = 2
trigger_parallel_dagrun_count_process_users = 5
trigger_parallel_dagrun_count_process_roles = 2
trigger_parallel_dagrun_count_process_service_centers = 2

# Log Settings
gather_user_logs_timeout_hours = 24
log_file_download_link_expiry_in_sec = 7 * 24 * 60 * 60  # 7 days

# Delta Detection (Event Log Polling)
DEFAULT_LOOKBACK_HOURS = 24  # Default lookback window for first run when no Airflow Variable exists
LAST_PROCESSED_TIME_VAR_PREFIX = 'guesttek_talent_user_import_last_processed_time'

# Talent API Settings
TALENT_API_PAGE_SIZE = 1000
EVENT_LOG_PAGE_SIZE = 1000

# Default Values
default_language = 'en'

# License Types (from mapper)
LICENSE_TOE = "TimeOff Enterprise"
LICENSE_WFM = "Workforce Management"
LICENSE_TBP = "TimeBill Plus"
LICENSE_TOE_WFM = "TOE|WFM"
LICENSE_TOE_WFM_TBP = "TOE|WFM|TBP"

# License URIs
LICENSE_URI_MAP = {
    "TimeOff Enterprise": "urn:replicon-saas:product:time-off-enterprise",
    "Workforce Management": "urn:replicon-saas:product:wfm-enterprise",
    "TimeBill Plus": "urn:replicon-saas:product:time-bill-plus",
}

# Supervisor Permission
SUPERVISOR_PERMISSION = "Supervisor"

# Replicon Custom Fields (OEF)
CUSTOM_FIELDS = [
    'Manually Updated',  # If "Yes", skip update for user
]

# Invalid date placeholder from Talent API
INVALID_DATE = "0000-00-00"

# User Status mapping
# Talent: user_deactivated = 0 means Enabled, 1 means Disabled
USER_STATUS_ENABLED = 0
USER_STATUS_DISABLED = 1

# Termination Reasons (if needed)
TERMINATION_REASONS = []

# Version
version = 'v1'

# Default Time Off Template
default_time_off_template = 'Time Off'
# Default Permission Sets for new users
default_permission_sets = ['Project Resource']
# Default Timesheet Period
default_timesheet_period = 'Semimonthly'
# Default Timesheet Approval Path
default_timesheet_approval_path = 'System Approval'