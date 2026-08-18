# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_iwo_leanstaffing.config import *

instance = 'sandbox2'
company_key = 'dxcsandbox2'
replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntC1'
sftp_conn_id = 'dxcsandbox2-sftp-628172_C1'
input_filepath = '/Test/Inbound/IWOLeanStaffing/Input'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
archive_filepath = '/Test/Inbound/IWOLeanStaffing/Archive'
log_filepath = '/Test/Inbound/IWOLeanStaffing/Logs'
