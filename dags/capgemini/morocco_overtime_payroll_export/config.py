region = 'eu-central-1'
environment = 'pre-production'

execution_timeout_days = 14
max_active_runs = 1
write_csv_thread_pool_size = 10

time_zone = "Etc/UTC"
schedule_interval = "0 1 * * *"
execution_timeout_mins_write_csv = 90

sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'

ma01_costcenter = "MA01 - Capgemini Technology Services Maroc S.A. | MA01"
ma02_costcenter = "MA02 - MG2 Engineering SA. | MA02"
ma03_costcenter = "MA03 - ALTRAN MAROC S.A.R.L.A.U | MA03"

export_headers = ["paycode", "employee_id", "format", "nbrbas", "hours", "entrydate", "entitlement"]
payroll_export_file_format = "Morocco Overtime Payroll Export"
paycodes = ('[MAR] Overtime 1.25', '[MAR] Overtime 1.5', '[MAR] Overtime 2.0', '[MAR] Overtime Standard Cost')

# This schedules need to be updated every year after the last schedule date based on customer request.
schedules = ["18/01/2025", "20/02/2025", "18/03/2025", "18/04/2025", "18/05/2025", "13/06/2025",
            "18/07/2025", "18/08/2025", "18/09/2025", "18/10/2025", "18/11/2025", "12/12/2025",
            "18/01/2026", "18/02/2026", "18/03/2026", "18/04/2026", "18/05/2026", "13/06/2026",
            "18/07/2026", "18/08/2026", "18/09/2026", "18/10/2026", "18/11/2026", "13/12/2026",
            "18/01/2027"
]
