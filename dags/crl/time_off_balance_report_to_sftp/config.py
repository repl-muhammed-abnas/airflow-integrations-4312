region = 'us-east-1'
environment = 'pre-production'


max_active_runs_master = 1
max_active_runs_user_child = 2
schedule_interval_can = '0 6 * * 1'
schedule_interval_usa = '0 6 * * 5'
time_zone = 'America/New_York'
execution_timeout_days = 14


USER_REPORTS_CAN = [
    "CRL - Time Off Accrual Report [CAN]"
]

USER_REPORTS_USA = [
    "CRL - Time Off Accrual Report [USA]"
]
