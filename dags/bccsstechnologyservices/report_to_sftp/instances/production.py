# pylint: disable=wildcard-import unused-wildcard-import
from bccsstechnologyservices.report_to_sftp.config import *

instance = 'production'
region = 'us-east-1'
environment = 'production'

company_key = 'BCCSSTechnologyServices'
replicon_conn_id = 'bccsstechnologyservices_replicon_RepliconSupport'
sftp_conn_id = 'bccsstechnologyservices_sftp_543674'

tenant_email ='{{ var.value.dagrun_internal_log_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
