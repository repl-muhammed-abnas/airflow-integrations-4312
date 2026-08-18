# pylint: disable=wildcard-import unused-wildcard-import
from briarhallllc.report_to_sftp.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'

company_key = 'briarhallllc'
replicon_conn_id = 'briarhallllc_replicon_azaremba'
sftp_conn_id = 'briarhallllc_sftp_511675'

tenant_email = "jkim@briarhall.com,azaremba@briarhall.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
