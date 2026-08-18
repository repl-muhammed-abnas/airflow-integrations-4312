
# pylint: disable=wildcard-import unused-wildcard-import
from datetime import timedelta
from dxctechnology.cwf_time_export_v2.config import *
region = 'us-east-2'
environment = 'production'

instance = 'DXCTechnology'
company_key = 'DXCTechnology'
replicon_conn_id = 'DXCTechnology_http_RepliconIntFG'
pgp_conn_id = 'pgp_dxctechnology_cwf_timeexport'

field_glass_sftp_conn_id = 'dxctechnology_sftp_628172_fieldglass'
field_glass_output_filepath = '/Production/Outbound/CWFTimesheets'

c1_sftp_conn_id = 'DXCTechnology-sftp-628172_C1'
c1_output_filepath = '/Production/Outbound/C1TimeExtract'

compass_sftp_conn_id = 'DXCTechnology-sftp-628172_COMPASS'
compass_output_filepath = '/Production/Outbound/COMPASSTimeExtract'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

c1_acknowledgement_email= 'mytimefunc@dxc.com'
compass_acknowledgement_email= 'compasshrtimeitl4@dxc.com'

c1_http_conn_id = 'dxctechnology_POP_C1TimeData'
compass_http_conn_id = 'dxctechnology_POP_CompassTimeData'

is_allowed_send_export_data = True

# Every Week once on Tuesday at 12:05 AM EST ( UTC 4:05 AM)
field_glass_schedule_interval = '05 04 * * TUE'

# Every Week once on Monday at 11:30 PM UTC
compass_master_schedule_interval = '30 23 * * MON'
utc_timezone= 'UTC'

field_glass_date_filter = {

    # - 7.days for prod
    'report_start_date': lambda: (get_today_utc_date(
    ) - timedelta(days=7)).strftime("%m/%d/%Y"),

    #  - 1 days for prod
    'report_end_date': lambda: (get_today_utc_date(
    ) - timedelta(days=1)).strftime("%m/%d/%Y"),

    # get_-12weeks_date
    # same for prod -84 days
    'timesheet_start_date': lambda: (get_today_utc_date(
    ) - timedelta(days=get_today_utc_date().weekday())
        - timedelta(days=84)).strftime("%m/%d/%Y"),
}
