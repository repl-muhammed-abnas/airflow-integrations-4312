region = 'eu-central-1'
environment = "pre-production"
time_zone = "Europe/Lisbon"

DATE_DEFAULT_FORMAT = "%Y/%m/%d"

ANNUAL_LEAVE_RTT_CARRIED_OVER = "FR - RTT reporté | RTT Carried Over"
ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS_CARRIED_OVER = "FR - RTT pour Forfait Jours Reportés | RTT for Forfait Jours Carried Over"

REQUIRED_TIMEOFF_TYPES = [
    ANNUAL_LEAVE_RTT_CARRIED_OVER,
    ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS_CARRIED_OVER
]

country = "France"
FRANCE_HOLIDAY_CALENDAR_NAME = "France"
WHIT_MONDAY_HOLIDAY_NAME = "Whit Monday"

whit_monday_balance_report = "**Time Off Balance Report France - Whit Monday**"
expected_report_columns = "User Name,Time Off Type,Time Off Balance,User Start Date,Employee ID,Login Name,Country (Current)"

schedule_interval = None  # Triggered by annual_leave_balance_transfer_france_v1 master DAG

execution_timeout_days = 14
max_active_runs_master = 1
max_active_runs_child = 5
parallel_dag_count = 10
