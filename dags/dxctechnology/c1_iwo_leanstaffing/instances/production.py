# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_iwo_leanstaffing.config import *

region = 'us-east-2'
environment = 'production'
instance = 'production'
company_key = 'dxctechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntC1'
sftp_conn_id = 'sftp_dxctechnology_c1'
input_filepath = '/Production/Inbound/IWOLeanStaffing/Input'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
archive_filepath = '/Production/Inbound/IWOLeanStaffing/Archive'
log_filepath = '/Production/Inbound/IWOLeanStaffing/Logs'
