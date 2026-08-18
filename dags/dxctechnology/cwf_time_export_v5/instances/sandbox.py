# pylint: disable=wildcard-import unused-wildcard-import
from datetime import timedelta
from dxctechnology.cwf_time_export_v5.config import *

region = 'us-east-2'
environment = 'pre-production'

instance = 'DXCSandbox'
company_key = 'DXCSandbox'

replicon_conn_id = 'dxcsandbox-replicon-RepliconIntFG'
pgp_conn_id = 'DXCSandbox_pgp_cwf_time_export'
pgp_conn_id_psa = 'dxctrial01_pgp_cwf_time_export_psa'

field_glass_sftp_conn_id = 'dxcsandbox-sftp-628172_fieldglass'
field_glass_output_filepath = '/Test/Outbound/CWFTimesheets'

c1_sftp_conn_id = 'dxcsandbox-sftp-628172_C1'
c1_output_filepath = '/Test/Outbound/C1TimeExtract'

compass_sftp_conn_id = 'dxcsandbox-sftp-628172_Compass'
compass_output_filepath = '/Test/Outbound/COMPASSTimeExtract'

gsap_sftp_conn_id = 'sftp_dxctechnology_gsap'
gsap_output_filepath = '/Outbound/'
gsap_http_conn_id = 'dxcsandbox_POQ_GSAPTimeData'

psa_sftp_conn_id = 'sftp_dxctechnology_psa'
psa_output_filepath = '/Test/Outbound/Time Export'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

c1_acknowledgement_email= 'mytimefunc@dxc.com'
compass_acknowledgement_email= 'compasshrtimeitl4@dxc.com'

c1_http_conn_id = 'dxcsandbox_POQ_C1TimeData'
compass_http_conn_id = 'dxcsandbox_POQ_CompassTimeData'

is_allowed_send_export_data = True

# trigger every day at 1am Mountain Time (US & Canada) in UTC 07AM
field_glass_schedule_interval = '00 07 * * *'
compass_master_schedule_interval = '30 0,6,12,18 * * *'

field_glass_date_filter = {

    # for sandbox yesterday_date
    # - 7.days for prod
    # for qa testing is today
    'report_start_date': lambda: (get_today_utc_date(
    ) - timedelta(days=get_today_utc_date().weekday()+7)).strftime("%m/%d/%Y"),

    # yesterday_date
    #  - 1 days for prod
    # for qa testing is today
    'report_end_date': lambda: (get_today_utc_date(
    ) - timedelta(days=get_today_utc_date().weekday()+1 % 7)).strftime("%m/%d/%Y"),

    # get_-12weeks_date
    # same for prod -84 days
    'timesheet_start_date': lambda: (get_today_utc_date(
    ) - timedelta(days=get_today_utc_date().weekday())
        - timedelta(days=84)).strftime("%m/%d/%Y"),
}

psa_wf39_sql_query = '''SELECT * FROM finaltimedata WHERE
            (((
                employeetypename LIKE '%Contractor%' AND companycodecode = 'C1' AND attendancetypecode NOT LIKE '%799%' AND ParentWBS IS Null
            )
            OR
            ((
                employeetypename LIKE '%Contractor%' AND companycodecode='COMPASS' AND attendancetypecode NOT LIKE '%799%' AND ParentWBS IS Null
            ))))
            AND
            ((
                psaflag IN ('x','X')
            )
            OR
            (
                organizationunitname IN ({{result("get_psa_orgs")}})
            ))
            ORDER BY CAST(hours as DECIMAL) ASC'''
