region = 'eu-central-1'
environment = "pre-production"
time_zone = "Europe/Lisbon"

DATE_DEFAULT_FORMAT = "%Y/%m/%d"

ANNUAL_LEAVE = "ESP - Vacaciones anuales (Annual leave)"
ANNUAL_LEAVE_CARRIED_OVER = "ESP - Vacaciones anuales transferidas (Annual Leave carryover)"

country = "Spain"

expected_report_columns = "User Name,Time Off Type,Time Off Balance,User Start Date,Employee ID,Login Name,Country (Current),Legal Entity Code (Current),Acquired Company"
schedule_interval_annual_leave = "0 1 1 1 *"

annual_leave_balance_report = "***Time Off Balance Report Spain - Annual Leave***"

execution_timeout_days = 14
max_active_runs_master = 1
max_active_runs_child = 5
process_balance_transfer_parallel_dagruns_count = 20
