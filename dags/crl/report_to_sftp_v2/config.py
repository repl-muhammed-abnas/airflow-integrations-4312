region = 'us-east-1'
environment = 'pre-production'


max_active_runs_master = 1
max_active_runs_project_child = 5
max_active_runs_user_child = 2
schedule_interval_project = '0 21 * * 5'
schedule_interval_user = '0 6 * * 4'
schedule_interval_user_ireland = '0 5 * * 4'
schedule_interval_user_israel = '0 5 * * 4'
schedule_interval_user_brazil = '0 7 * * 4'
schedule_interval_user_switzerland = '0 5 * * 4'
schedule_interval_user_germany = '0 4 * * 4'


schedule_interval_user_uk = '0 21 * * 5'
schedule_interval_network_file_draft = '0 9 * * 3'
schedule_interval_dm_emp_new = '0 9 * * 3'

# Time Zone
time_zone = 'America/New_York'
utc_time_zone = 'UTC'
brazil_time_zone = 'America/Sao_Paulo'
germany_time_zone = 'Europe/Berlin'
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
    "CRL Project reconciliation - 4650",
    "CRL Project reconciliation - 3500",
    "CRL Project reconciliation - 3540",
    "CRL Project reconciliation - 4200",
    "CRL Project reconciliation - 4525",
    "CRL Project reconciliation - 5515",
    "CRL Project reconciliation - 8200"
]

PROJECT_REPORTS_UK = [
    "CRL UK Project reconciliation - 3000",
    "CRL UK Project reconciliation - 3040",
    "CRL UK Project reconciliation - 3050",
    "CRL UK Project reconciliation - 3080",
    "CRL UK Project reconciliation - 3300",
]

PROJECT_REPORTS_GERMANY = [
    "CRL Germany Project reconciliation - 4000",
    "CRL Germany Project reconciliation - 4020",
    "CRL Germany Project reconciliation - 4200",
]

User_Reports = [
    "CRL User Reconciliation - Canada",
    "CRL User Reconciliation - US"
]

USER_REPORT_IRELAND = [
    "CRL User Reconciliation - Ireland"
]

USER_REPORT_BRAZIL = [
    "CRL User Reconciliation - Brazil"
]

USER_REPORT_ISRAEL = [
    "CRL User Reconciliation - Israel"
]

USER_REPORT_SWITZERLAND = [
    "CRL User Reconciliation - Switzerland"
]

USER_REPORT_UK = [
    "CRL User Reconciliation - UK",
]

USER_REPORT_GERMANY = [
    "CRL User Reconciliation - Germany"
]

DM_EMP_New_Report = "DM_EMP_New"
DM_EMP_New_Filename = "Replicon_Emp_Pay_details"

Network_File_Draft_Report = "Network_File_Draft"
Network_File_Draft_Filename = "Replicon_Network_Time_Allocation"
