
# pylint: disable=wildcard-import unused-wildcard-import
from datetime import timedelta
from dxctechnology.cwf_time_export_v2.config import *
region = 'us-east-2'
environment = 'pre-production'

instance = 'DXCTrial01'
company_key = 'DXCTrial01'
replicon_conn_id = 'dxctrial01'

pgp_conn_id = 'dxctrial01_pgp_cwf_time_export'

field_glass_sftp_conn_id = 'sftp_internal'
field_glass_output_filepath = '/DXC/C1WBS/logs'

c1_sftp_conn_id = 'sftp_internal'
c1_output_filepath = '/DXC/C1WBS/logs'

compass_http_conn_id = 'http_conn_id'
c1_http_conn_id = 'http_conn_id'

compass_sftp_conn_id = 'sftp_internal'
compass_output_filepath = '/DXC/C1WBS/logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
exception_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

c1_acknowledgement_email= '{{ var.value.dagrun_internal_testing_email }}'
compass_acknowledgement_email= '{{ var.value.dagrun_internal_testing_email }}'

execution_timeout_days = 14
sftp_upload_path = '/DXC/C1WBS/logs'

# trigger every day at 1am Mountain Time (US & Canada) in UTC 07AM
field_glass_schedule_interval = '00 07 * * *'
compass_master_schedule_interval = '30 0,6,12,18 * * *'

# while doing the QA testing we have to skip the uploading data to customer api
is_allowed_send_export_data = False

field_glass_date_filter = {

    # for sandbox yesterday_date
    # - 7.days for prod
    # for qa testing is today
    'report_start_date': lambda: (get_today_utc_date(
    ) - timedelta(days=1)).strftime("%m/%d/%Y"),

    # yesterday_date
    #  - 1 days for prod
    # for qa testing is today
    'report_end_date': lambda: (get_today_utc_date(
    ) - timedelta(days=1)).strftime("%m/%d/%Y"),

    # get_-12weeks_date
    # same for prod -84 days
    'timesheet_start_date': lambda: (get_today_utc_date(
    ) - timedelta(days=get_today_utc_date().weekday())
        - timedelta(days=84)).strftime("%m/%d/%Y"),
}
