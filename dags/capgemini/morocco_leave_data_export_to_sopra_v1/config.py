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
expected_approved_report_columns = "Employee ID;Booking Start Date;Booking End Date;Time Off Type;01 - Half Day Leave (Start Day);02 - Half Day Leave (End Day);Time Off Hrs;Approval Status;Booking Uri;Bookingdays;Cost Center"
expected_deleted_report_columns = "Employee ID;Current Start Date;Current End Date;Current Time Off Type;Action;Booking Uri;Cost Center"

export_headers = ["paycode", "employee_id", "booking_start_date", "booking_end_date", "day_start_indicator",
    "day_end_indicator", "hours", "short_id", "transaction_type", "horodatage", "initialorextension", "workedstartday", "cost_center"]

ma01_costcenter = "MA01 - Capgemini Technology Services Maroc S.A. | MA01"
ma02_costcenter = "MA02 - MG2 Engineering SA. | MA02"
ma03_costcenter = "MA03 - ALTRAN MAROC S.A.R.L.A.U | MA03"

# This schedules need to be updated every year after the last schedule date based on customer request.
# When adding a new schedule list, add the last schedule date of previous year to first element of
#   new list as we export data from previous scheduled date to current scheduled date-1.
schedules = ["18/01/2025", "20/02/2025", "18/03/2025", "18/04/2025", "18/05/2025", "13/06/2025",
            "18/07/2025", "18/08/2025", "18/09/2025", "18/10/2025", "18/11/2025", "12/12/2025",
            "18/01/2026", "18/02/2026", "18/03/2026", "18/04/2026", "18/05/2026", "13/06/2026", 
            "18/07/2026", "18/08/2026", "18/09/2026", "18/10/2026", "18/11/2026", "13/12/2026", 
            "18/01/2027"
]
schedule_interval = "0 1 * * *"
