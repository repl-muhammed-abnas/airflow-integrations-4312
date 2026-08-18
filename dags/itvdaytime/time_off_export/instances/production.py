# pylint: disable=wildcard-import unused-wildcard-import
from itvdaytime.time_off_export.config import *

region = 'eu-central-1'
environment = 'production'

instance = 'production'
company_key = 'itvdaytime'

schedule_interval='00 01 * * *'

max_active_runs = 5

replicon_conn_id = 'replicon_itvdaytime_radmin'
sftp_conn_id = 'sftp_itvdaytime_563217'

report_name = "***Replicon-Timeoff Export Integration Base Report"

output_file_path = '/Production/Export/Time Off/'

tenant_email = "technologyservicedesk@itv.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
