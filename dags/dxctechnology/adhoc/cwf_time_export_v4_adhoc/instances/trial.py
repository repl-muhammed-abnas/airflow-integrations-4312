# pylint: disable=wildcard-import unused-wildcard-import
from datetime import timedelta
from dxctechnology.adhoc.cwf_time_export_v4_adhoc.config import *

region = 'us-east-2'
environment = 'pre-production'

instance = 'trial'
company_key = 'DXCTrial01'
replicon_conn_id = 'dxctrial01'

pgp_conn_id = 'dxctrial01_pgp_cwf_time_export'
pgp_conn_id_psa = 'dxctrial01_pgp_cwf_time_export_psa'

field_glass_sftp_conn_id = 'sftp_useast2'
field_glass_output_filepath = '/Trial/Export/C1CWF'

gsap_sftp_conn_id = 'sftp_useast2'
gsap_output_filepath = '/Trial/Export/C1CWF'
gsap_http_conn_id = 'http_conn_id'

psa_sftp_conn_id = 'sftp_useast2'
psa_output_filepath = '/Trial/Export/CWFPSA'

c1_sftp_conn_id = 'sftp_useast2'
c1_output_filepath = '/Trial/Export/C1CWF'

compass_http_conn_id = 'http_conn_id'
c1_http_conn_id = 'http_conn_id'

compass_sftp_conn_id = 'sftp_useast2'
compass_output_filepath = '/Trial/Export/C1CWF'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
exception_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

c1_acknowledgement_email= '{{ var.value.dagrun_internal_testing_email }}'
compass_acknowledgement_email= '{{ var.value.dagrun_internal_testing_email }}'

execution_timeout_days = 14
sftp_upload_path = '/Trial/Export/C1CWF'

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

cwf_fieldglass_report_start_date = f'cwf_fieldglass_report_start_date_{instance}'
cwf_fieldglass_report_end_date =  f'cwf_fieldglass_report_end_date_{instance}'
