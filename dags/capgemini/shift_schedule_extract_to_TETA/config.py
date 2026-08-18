region = 'eu-central-1'
environment = 'pre-production'

max_active_runs = 1
max_active_load_users_shifts = 10
max_active_child_runs = 4
time_zone = "Etc/UTC"
schedule_interval = '0 17 * * *'

dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

shift_assignment_report = 'Poland Shift Schedule Report'
# pylint: disable=line-too-long
shift_assignment_report_columns = 'Local ID;Card Number;Shift Name;Shift Date;Shift Start Time;Shift End Time;Number of Hours'
export_columns = ["Local_ID", "CARD_NO", "DAY", "start_hour", "End_hour", "nominal_time", "day_type"]
no_of_months_shift_data_to_export = 4 # variable to derive current + future months shift data

execution_timeout_mins_write_csv = 90
execution_timeout_days = 14
gather_logs_timeout_hours = 12

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'
