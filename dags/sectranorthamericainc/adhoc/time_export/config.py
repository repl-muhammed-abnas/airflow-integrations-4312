region = 'us-east-1'
environment = "pre-production"

master_max_active_run = 1
execution_timeout_days = 14
generate_token_ttl_days = 1
post_time_export_data_max_active_run = 5

time_zone = "Etc/UTC"
daily_run_schedule_interval = "0 */2 * * *"

time_export_file_format = "*** Time Export"
sumo_conn_id = 'sumologic-exportlogger'
timesheet_day_report_name = "*** Time Export TS Day"

# pylint: disable=line-too-long
expected_report_columns = "Login Name,Project Name,Project Code,Project Location,Project Location Code,Billing Rate Name,Billing rate currency ,Billing rate amount,Approval Status"
