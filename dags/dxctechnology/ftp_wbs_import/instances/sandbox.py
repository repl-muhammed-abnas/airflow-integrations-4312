# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.ftp_wbs_import.config import *

instance = 'dxcsandbox'
company_key = 'dxcsandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntFTP'
sftp_conn_id = "dxcsandbox-sftp-628172_FTP"
input_filepath = "/Test/Inbound/FTPWBSMaster/Input"
archive_filepath = "/Test/Inbound/FTPWBSMaster/Archive"
log_filepath = "/Test/Inbound/FTPWBSMaster/Logs"
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
