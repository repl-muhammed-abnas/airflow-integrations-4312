region = 'us-east-1'
environment = "pre-production"
disable_user_master_dag_active_runs = 1
disable_user_child_dag_active_runs = 5
execution_timeout_days = 14
pacific_timezone = 'America/Los_Angeles'
report_name = '***Disable User Template - For User Import'
disable_user_master_dag_interval = '0 1 * * *'

sumo_conn_id = 'sumologic-dagrunlogger'
IGNORE_STATUS_ZERO_ACCRUAL = ['Suspended']
MANNUAL_TIMEOFF_TYPES = ['[CAN] Vacances 2023/Vacation 2023 Carry over','[CAN] Vacances 2023/SC Vacation 2023 Carry over',
    "[CAN] Vacances/Vacation June 23 - Dec 23","[CAN] Vacances/Vacation May 22 - June 23",
    "[CAN] PMSD-Maternite Lésion profess. |Autres/Occupational Injury - other/ CNESST", "[CAN] Exception vacances/Exception Vacation" ]