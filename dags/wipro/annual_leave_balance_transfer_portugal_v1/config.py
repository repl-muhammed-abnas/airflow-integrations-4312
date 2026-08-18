region = 'eu-central-1'
environment = "pre-production"
time_zone = "Europe/Lisbon"

DATE_DEFAULT_FORMAT = "%Y/%m/%d"

ANNUAL_LEAVE = "POR - Férias (Annual Leave)"
ANNUAL_LEAVE_TRAVELPORT = "POR - Férias anuais Travelport (Annual Leave Travelport)"
ANNUAL_LEAVE_CARRIED_OVER = "POR - Férias anuais transitadas (Annual Leave Carried Over)"
ANNUAL_LEAVE_LAPSED = "POR - Férias anuais vencidas (Annual Leave Lapsed)"

country = "Portugal"

expected_report_columns = "User Name,Time Off Type,Time Off Balance,User Start Date,Employee ID,Login Name,Country (Current)"

schedule_interval_annual_leave = "0 1 1 1 *"
schedule_interval_annual_leave_carried_over = "0 0 1 5 *"
schedule_interval_annual_leave_carried_over_probation_users = "0 0 1 7 *"

annual_leave_balance_report = "***Time Off Balance Report - Annual Leave***"

execution_timeout_days = 14
max_active_runs_master = 1
max_active_runs_child = 5
process_users_for_timeoff_balance_transfer_parallel_dagruns_count = 20
