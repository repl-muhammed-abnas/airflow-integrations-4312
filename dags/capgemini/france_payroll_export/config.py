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

sopra_export_headers = ["paycode", "employee_id", "format", "hours", "monsal", "entrydate", "entitlement"]
gfs_export_headers = ["Reference", "Transaction_source", "Batch_name", "Employee_number",
    "Local Employee Number", "Expenditure_item_date", "Pay Code", "Project_number", "Task_number",
    "Expenditure_type", "Non Labor resource", "Non Labor resource_org_name", "Organization_name",
    "Quantity", "Expenditure_comment", "DFF : Start_date", "DFF: End_date", "Quantity in days",
    "External application unit of measure for time entry", "Attribute3", "Attribute4", "Attribute5",
    "Attribute6", "Attribute7", "Attribute8", "Attribute9", "Nb_hours_sup", "Raw cost", "Raw cost rate", "Billable Flag"]

payroll_export_file_format = "France Payroll Export"

# This schedules need to be updated every year after the last schedule date based on customer request.
schedules = ["28/01/2026", "25/02/2026", "30/03/2026", "28/04/2026", "27/05/2026", "17/06/2026",
            "29/07/2026", "31/08/2026", "28/09/2026", "28/10/2026", "30/11/2026", "16/12/2026"]
