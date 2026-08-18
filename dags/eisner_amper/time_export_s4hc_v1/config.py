environment = 'pre-production'
region = 'us-east-1'

execution_timeout_days = 14

max_active_runs_master = 1
max_active_runs_child = 10
schedule_interval = '30 0 * * *'

extract_time_entry_report = 'Time Entry Exports V1'

time_zone = "America/New_York"

error_template = '{{ get_error_message() }}'

thread_pool_size_write_csv = 10

# S4HC Constants
CONTROLLING_AREA = 'A000'
TIMESHEET_OPERATION = 'C'
HOURS_UNIT = 'H'
APPROVAL_STATUS = '30'
NON_BILLABLE_CODE = 'NON_BILL'
BILLABLE_ENTRY_TYPE = 'Billable Only'
NON_BILLABLE_ENTRY_TYPE = 'Non-Billable'

# Project Profiles
PROJECT_PROFILE_P001 = 'P001'
PROJECT_PROFILE_YP02 = 'YP02'
PROJECT_PROFILE_YP04 = 'YP04'

# Service Line Extraction Indices (from CostCenterCode) - slice(4,3)
SERVICE_LINE_START_INDEX = 4
SERVICE_LINE_END_INDEX = 7

# Location Code Extraction Indices (from CostCenterCode) - slice(7,3)
LOCATION_CODE_START_INDEX = 7
LOCATION_CODE_END_INDEX = 10

# Batch Settings
child_dag_batch_size = 5000
chunk_size_write_csv = 2000
can_run_batch_task_var_name = 'eisner_amper_time_export_s4hc_can_run_batch_task'
#