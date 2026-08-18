region = 'us-east-1'
environment = 'pre-production'


max_active_runs_master = 1
max_active_runs_project_child = 5
max_active_runs_user_child = 2
schedule_interval_project = '0 21 * * 5'
schedule_interval_user = '0 6 * * 4'
schedule_interval_network_file_draft = '0 9 * * 3'
schedule_interval_dm_emp_new = '0 9 * * 3'
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
    "CRL Project reconciliation - 4650",
    "CRL Project reconciliation - 3500",
    "CRL Project reconciliation - 3540",
    "CRL Project reconciliation - 4200",
    "CRL Project reconciliation - 4525",
    "CRL Project reconciliation - 5515",
    "CRL Project reconciliation - 8200"
]

User_Reports = [
    "CRL User Reconciliation - Canada",
    "CRL User Reconciliation - US"
]

DM_EMP_New_Report = "DM_EMP_New"
DM_EMP_New_Filename = "Replicon_Emp_Pay_details"

Network_File_Draft_Report = "Network_File_Draft"
Network_File_Draft_Filename = "Replicon_Network_Time_Allocation"
