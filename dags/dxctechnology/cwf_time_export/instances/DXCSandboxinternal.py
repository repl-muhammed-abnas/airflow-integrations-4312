
# pylint: disable=wildcard-import unused-wildcard-import
from datetime import timedelta
from dxctechnology.cwf_time_export.config import *
from dxctechnology.cwf_time_export import request_payload
region = 'us-east-2'
environment = 'pre-production'

instance = 'DXCSandboxinternal'
company_key = 'DXCSandboxinternal'
replicon_conn_id = 'dxcsandboxinternal'

pgp_conn_id = 'dxcsandboxinternal_pgp_cwf_time_export'

field_glass_sftp_conn_id = 'dxcsandboxinternal_sftp_cwf_time_export'
field_glass_output_filepath = '/dxcsandboxinternal_cwf_time_export/Output'

c1_sftp_conn_id = 'dxcsandboxinternal_sftp_cwf_time_export'
c1_output_filepath = '/dxcsandboxinternal_cwf_time_export/Output'

compass_sftp_conn_id = 'dxcsandboxinternal_sftp_cwf_time_export'
compass_output_filepath = '/dxcsandboxinternal_cwf_time_export/Output'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

c1_http_conn_id = 'dxcsandboxinternal_http_cwf_c1_timeexport'
compass_http_conn_id = 'dxcsandboxinternal_http_cwf_compass_timeexport'

# trigger every day at 1am Mountain Time (US & Canada) in UTC 07AM
field_glass_schedule_interval = '00 07 * * *'
compass_master_schedule_interval = '30 0,6,12,18 * * *'

field_glass_date_filter = {

    # for sandbox yesterday_date
    # - 7.days for prod
    # for qa testing is today
    'report_start_date': lambda: (request_payload.get_today_utc_date(
    ) - timedelta(days=0)).strftime("%m/%d/%Y"),

    # yesterday_date
    #  - 1 days for prod
    # for qa testing is today
    'report_end_date': lambda: (request_payload.get_today_utc_date(
    ) - timedelta(days=0)).strftime("%m/%d/%Y"),

    # get_-12weeks_date
    # same for prod -84 days
    'timesheet_start_date': lambda: (request_payload.get_today_utc_date(
    ) - timedelta(days=request_payload.get_today_utc_date().weekday())
        - timedelta(days=84)).strftime("%m/%d/%Y"),
}
