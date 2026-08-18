region = 'eu-central-1'
environment = 'pre-production'

execution_timeout_days = 14
max_active_runs = 1
write_csv_thread_pool_size = 10

time_zone = "Etc/UTC"

execution_timeout_mins_write_csv = 90

sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'

# pylint: disable=line-too-long
expected_report_columns = "Employee ID;Local Employee Number;Time Off Type;Time Off Type Description;Current Year Balance;Leave Availed;Leave Balance;Cost Center (Current) (Full Path)"

export_headers = ["paycode", "employee_id", "current_year_balance", "leaves_availed", "leave_balance",
    "transaction_type", "horodatage", "cost_center_fullpath"]

ma01_costcenter = "MA01 - Capgemini Technology Services Maroc S.A. | MA01"
ma02_costcenter = "MA02 - MG2 Engineering SA. | MA02"
ma03_costcenter = "MA03 - ALTRAN MAROC S.A.R.L.A.U | MA03"

# This schedules need to be updated every year after the last schedule date based on customer request
schedules = ["24/01/2025", "24/02/2025", "24/03/2025", "22/04/2025", "22/05/2025", "13/06/2025",
            "22/07/2025", "22/08/2025", "22/09/2025", "22/10/2025", "22/11/2025", "12/12/2025",
            "22/01/2026", "22/02/2026", "22/03/2026", "22/04/2026", "22/05/2026", "13/06/2026",
            "22/07/2026", "22/08/2026", "22/09/2026", "22/10/2026", "22/11/2026", "13/12/2026",
            "22/01/2027"
]
schedule_interval = "0 2 * * *"
