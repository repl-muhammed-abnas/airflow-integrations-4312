region = 'eu-central-1'
environment = "pre-production"
time_zone = "Europe/Lisbon"

DATE_DEFAULT_FORMAT = "%Y/%m/%d"

ANNUAL_LEAVE = "FR - Congé annuel | Annual Leave"
ANNUAL_LEAVE_CARRIED_OVER = "FR - Congés annuels reportés | Annual Leave Carried Over"

ANNUAL_LEAVE_ACCRUED = "FR - Congés annuel en acquisition | Annual Leave Accrued"

ANNUAL_LEAVE_SENIORITY_DAYS = "FR - Congés annuels Journées d’ancienneté | Annual Leave Seniority Days"
ANNUAL_LEAVE_SENIORITY_DAYS_CARRIED_OVER = "FR - Congé annuel Jours d'ancienneté reportés | Annual Leave Seniority Days Carried Over"

ANNUAL_LEAVE_RTT = "FR - RTT"
ANNUAL_LEAVE_RTT_CARRIED_OVER = "FR - RTT reporté | RTT Carried Over"

ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS = "FR - RTT pour Forfait Jours | RTT for Forfait Jours"
ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS_CARRIED_OVER = "FR - RTT pour Forfait Jours Reportés | RTT for Forfait Jours Carried Over"

REQUIRED_TIMEOFF_TYPES = [
    ANNUAL_LEAVE, ANNUAL_LEAVE_CARRIED_OVER, ANNUAL_LEAVE_ACCRUED,
    ANNUAL_LEAVE_SENIORITY_DAYS, ANNUAL_LEAVE_SENIORITY_DAYS_CARRIED_OVER,
    ANNUAL_LEAVE_RTT, ANNUAL_LEAVE_RTT_CARRIED_OVER,
    ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS, ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS_CARRIED_OVER
]

country = "France"

expected_report_columns = "User Name,Time Off Type,Time Off Balance,User Start Date,Employee ID,Login Name,Country (Current)"
schedule_interval_annual_leave = "0 1 1 6 *"

annual_leave_balance_report = "**Time Off Balance Report France - Annual Leave**"

execution_timeout_days = 14
max_active_runs_master = 1
max_active_runs_child = 5
parallel_transfer_dag_count = 10
