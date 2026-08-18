from datetime import datetime, timezone

region = 'us-east-2'
environment = 'pre-production'

dag_max_active_runs = 1
dag_max_active_tasks = 128
field_glass_report_name = 'CWF Time - Fieldglass Gsap'
field_glass_timesheet_period_report_name = 'CWF Time - Fieldglass Gsap Timesheets'

input_date_format = '%d %B %Y'  # date format in 3 April 2021
output_date_format = '%m/%d/%Y'
entry_date_format = '%d/%m/%Y'
report_date_format = '%d %B %Y '
# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'
exception_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
execution_timeout_days = 14
utc_timezone= 'UTC'
est_timezone = 'EST'

rate_types_list = ['Straight Time', 'Double Time', 'Overtime']
expected_timesheet_period_report_columns = 'User Last Name,User First Name,Timesheet Period,Timesheet Start Date,Timesheet End Date,Total Hrs (In Period),Company Code Code (Current),Login Name,Location (Current),Cost Center (Current),UserUri,Company Code (Current),Employee ID,Scheduled Hrs (In Period)'

write_csv_threadpool_size = 10

def get_today_utc_date():
    return datetime.now(timezone.utc)
