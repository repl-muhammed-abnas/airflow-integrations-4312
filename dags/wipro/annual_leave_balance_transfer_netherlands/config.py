region = 'eu-central-1'
environment = "pre-production"
time_zone = "Etc/UTC"

DATE_DEFAULT_FORMAT = "%Y/%m/%d"

ANNUAL_LEAVE = "NL ODR-Annual leave"
ANNUAL_LEAVE_ADDITIONAL = "NL ODR-Additional Annual leave"
ANNUAL_LEAVE_CARRIED_OVER = "NL ODR-Annual leave Carried Over"

country = "Netherlands"

expected_report_columns = "User Name,Time Off Type,Time Off Balance,User Start Date,Employee ID,Login Name,Country (Current)"

schedule_interval_annual_leave = "0 1 1 1 *"
schedule_interval_annual_leave_carried_over = "0 0 1 5 *"
schedule_interval_annual_leave_carried_over_probation_users = "0 0 1 7 *"

annual_leave_balance_report = "***Time Off Balance Report NL - Annual Leave***"

execution_timeout_days = 14
max_active_runs_master = 1
max_active_runs_child = 5
process_users_for_timeoff_balance_transfer_parallel_dagruns_count = 20
time_off_accrual_script_name = 'Yearly/Monthly Accrual with Expiry & Rounding'
