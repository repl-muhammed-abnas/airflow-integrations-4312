region = 'eu-central-1'
environment = 'pre-production'

new_users_schedule_interval = '0 1 * * *'
time_zone = "UTC"

execution_timeout_days = 14

sumo_conn_id = 'sumologic-dagrunlogger'

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'

optional_holiday_timeoff_type_name = '[IND] - Optional Holiday'
user_details_report = "User Details Report - India"
expected_user_details_report_columns = "Employee ID,UserUri,User Start Date,Location (Current) (Full Path)"

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

# the below values depends on the airflow-integrations\dags\capgemini\optional_holidays_auto_population_india_v1\config.py
e1_schedule = "02/01"
e2_schedule = "08/01"
