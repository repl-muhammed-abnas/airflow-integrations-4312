region = 'us-east-1'
environment = 'pre-production'
timesheet_posting_config = {
    'recurring': 'N',
    'selected': 'N',
    'posted': 'N',
    'tsmaster_status': 'N'
}
export_filter_timesheet_status = 'approved'
export_filter_export_status = 'none'
export_filter_time_entry_types = 'worked-time,time-off'

schedule_interval = '0 */3 * * *'
schedule_interval_time_category_sync = '0 2 * * *'

time_zone = 'US/Eastern'
execution_timeout_days = 14
child_dag_max_active_runs = 2

#If you change below format, change in initial setup config too
replicon_date_format = "%b %d, %Y"
should_post_timeentry_comments = False

tenant_email = 'MPTeamReplicon@deltek.com'
internal_email = 'MPTeamReplicon@deltek.com'
max_active_runs_per_employee = 5
max_active_runs_per_employee_timesheet = 10
max_active_runs_time_category_sync_for_user = 5

lookback_days = 90
lookahead_days = 30

laborcode_delimiter = ''


bucket_name = 'airflow-systemtest'
s3_upload_filepath = 'TimeCategoryValues/'
timecategory_file_name = '_users_timecategory_list.csv'
aws_conn_id = 'vp_aws_conn'

timesheet_field_oef_name_for_lc = 'Labor Codes'
enable_budget_labor_codes_level = False
budget_labor_codes_level = "Task" # Task / TimesheetFields
