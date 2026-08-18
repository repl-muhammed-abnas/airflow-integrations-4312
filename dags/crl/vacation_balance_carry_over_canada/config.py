region = 'us-east-1'
environment = "pre-production"

execution_timeout_days = 14
max_active_runs_master = 1
max_active_runs_child = 5
trigger_parallel_count = 10

schedule_interval="0 8 1 1 *"

time_zone = "US/Eastern"
balance_carry_over_report = "Time off Balance carryover - Base report"
expected_report_columns = "Login Name,User URI,Time Off Type,Units,Time Off Balance,Standard Hours"

VACATION_TIMEOFF = "[CAN] Vacances/Vacation"
VACATION_TIMEOFF_CARRY_OVER = "[CAN] Vacances précédentes reportées/Vacation carry over"

