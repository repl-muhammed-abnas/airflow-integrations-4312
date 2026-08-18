region = 'us-east-1'
environment = 'pre-production'


max_active_runs_master = 1
max_active_runs_project_child = 5
max_active_runs_user_child = 2
schedule_interval_project = '0 21 * * 5'
schedule_interval_user = '0 6 * * 4'
time_zone = 'America/New_York'
execution_timeout_days = 14


Project_Reports = [
    "CRL Project reconciliation - 1000",
    "CRL Project reconciliation - 1100",
    "CRL Project reconciliation - 1250",
    "CRL Project reconciliation - 1400",
    "CRL Project reconciliation - 1520",
    "CRL Project reconciliation - 1600",
    "CRL Project reconciliation - 1700",
    "CRL Project reconciliation - 2000",
    "CRL Project reconciliation - 2001",
    "CRL Project reconciliation - 2100",
    "CRL Project reconciliation - 3050",
    "CRL Project reconciliation - 3300",
    "CRL Project reconciliation - 4520",
    "CRL Project reconciliation - 4650"
]

User_Reports = [
    "CRL User Reconciliation - Canada",
    "CRL User Reconciliation - US"
]

