region = 'eu-central-1'
environment = 'pre-production'

schedule_interval = "0 1 1 2,8 *"
timeoff_status_update_schedule_interval = '*/30 * * * *'
time_zone = "UTC"

execution_timeout_days = 14

sumo_conn_id = 'sumologic-dagrunlogger'

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'

optional_holiday_timeoff_type_name = '[IND] - Optional Holiday'
optional_holiday_balance_report = 'Optional Holiday Balance Report'
expected_balance_report_columns = 'User Name,Time Off Type,Time Off Balance,useruri,Employee ID'

# pylint: disable=line-too-long
optional_holiday_status_report = 'Optional Holiday Booking Status'
expected_status_report_columns = 'User Name,Time Off Type,Time Off Days,Approval Status,Booking Start Date,timeoffuri,Submitted By Employee Id,Submitted By Employee Name'

e1_schedule_daterange = {
    "start_month": 2,
    "start_day": 1,
    "end_month": 6,
    "end_day": 30
}
e2_schedule_daterange = {
    "start_month": 8,
    "start_day": 1,
    "end_month": 12,
    "end_day": 31
}

e1_schedule = "02/01"
e2_schedule = "08/01"
