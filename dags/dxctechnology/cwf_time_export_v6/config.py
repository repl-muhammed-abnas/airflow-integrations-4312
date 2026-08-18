from datetime import datetime, timezone

region = 'us-east-2'
environment = 'pre-production'

dag_max_active_runs = 10
dag_max_active_tasks = 128
field_glass_report_name = 'CWF Time - Fieldglass Gsap'

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

def get_today_utc_date():
    return datetime.now(timezone.utc)
