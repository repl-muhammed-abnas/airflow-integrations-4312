"""
Workday-Replicon User Import Integration - Configuration Module

This module contains shared configuration values for all DAGs and instances
in the Unisys Workday User Import integration.

Configuration Categories:
    - Company Information: Region and environment settings
    - Execution Settings: Timeouts, intervals, and retry configurations
    - Max Active Runs: Concurrent execution limits for each DAG
    - Parallel Processing: Parallel dagrun counts for batch operations
    - User Types: Employee classification constants
    - Default Values: Language and timezone defaults
    - License Types: Available Replicon license types
    - Input File Configuration: CSV header and custom field definitions
    - Business Rules: Location exclusions and cost center exceptions

Constants:
    execution_timeout_days (int): Maximum DAG execution timeout (14 days)
    master_dag_interval (int): Master DAG scheduling interval in seconds (30s)
    log_file_download_link_expiry_in_sec (int): Log file URL expiry (7 days)
    OUT_OF_SCOPE_LOCATIONS (list): Locations excluded from user imports
    EXCEPTIONS_CO_CODE (list): Company Code|Cost Center combinations with special handling
"""

# Company Information
region = "us-east-1"
environment = "pre-production"
version = 'v1'

# Execution Settings
execution_timeout_days = 14
master_dag_interval = 30
file_sensor_timeout = 10

max_active_run_master = 1
max_active_runs_process_groups = 4
max_active_runs_process_users = 20
max_active_runs_process_divisions = 4
max_active_runs_process_schedule = 4
create_employeetypes_child_max_active_runs = 4
max_active_runs_process_locations = 4
schedule_dag_max_active_runs = 4
max_active_runs_process_new_users = 20
max_active_runs_process_update_users = 20
max_active_runs_process_supervisor = 20
gather_user_logs_timeout_hours = 24
max_active_runs_process_log_generation = 1


trigger_parallel_dagrun_count_process_locations = 2
trigger_parallel_dagrun_count_process_usertypes = 2
trigger_parallel_dagrun_count_process_divisions = 2
trigger_parallel_dagrun_count_process_schedules = 2
trigger_parallel_dagrun_count_process_users = 20

log_file_download_link_expiry_in_sec = 7*24*60*60

disable_master_dag_interval = "0 1 * * *"
disable_master_dag_active_runs = 1

# User Types
user_type_employee = 'Employee'
user_type_contingent = 'Contingent Worker'

# Default Values
default_language = 'en'
default_timezone_usa = 'America/New_York'

# License Types
licenses = ["TOE", "WFM", "Polaris PSA"]

input_file_header = 'Employee ID,Login Name,First Name,Last Name,Email,Supervisor ID,Location,Location Description,User Type,CompanyCode_CostCenter,Company Code Description,Cost Center Description,Department ID,Change Effective Date,Schedule,Start Date,End Date,Purchase Order ID,User Status,Job Code,Pay Group,Fusion Business Unit,Union_Employee,Premium_Pay_Eligible,Latest_Leave_Start_Date,Latest_Leave_End_Date,Shift_Eligible'
CUSTOM_FIELDS = [
    'Department ID', 'Change Effective Date', 'User Status', 'Job Code',
    'Pay Group', 'Fusion Business Unit', 'Union Employee', 'Premium Pay Eligible',
    'Leave Start Date', 'Leave End Date', 'Shift Eligible', 'Supplier Number', 'Leave Type'
]

OUT_OF_SCOPE_LOCATIONS = [
    'costa rica', 'peru', 'mexico', 'japan',
    'malaysia', 'philippines', 'lithuania', 'puerto rico',
    'ireland', 'luxembourg'
]

EXCEPTIONS_CO_CODE = [
    "200|4015",
    "232|4015",
    "256|4015",
    "281|4015",
    "282|4015",
    "283|4015",
    "554|4015",
    "602|4919",
    "602|4926",
    "602|4927",
    "602|4928"
]

PROCESS_USER_BATCH_COUNT = 20

user_disable_report_name = "User with end date"
