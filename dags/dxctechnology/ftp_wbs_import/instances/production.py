# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.ftp_wbs_import.config import *

environment = 'production'
instance = 'DXCTechnology'
company_key = 'DXCTechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntFTP'
sftp_conn_id = "dxctechnology-sftp-628172_FTP"
input_filepath = "/Production/Inbound/FTPWBSMaster/Input"
archive_filepath = "/Production/Inbound/FTPWBSMaster/Archive"
log_filepath = "/Production/Inbound/FTPWBSMaster/Logs"
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
