# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.ftp_time_export_v3.config import *

instance = "sandbox"
company_key = 'DXCSandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntFTP'
sumo_conn_id = 'sumologic-exportlogger'
sftp_conn_id = 'dxcsandbox-sftp-628172_FTP'
http_conn_id = 'dxcsandbox-dxc-REPLICON_POQ'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
exception_email = '{{ var.value.dagrun_failure_alert_email }}'
sftp_upload_path = '/Test/Outbound/FTPTimeExtract'
row_threshold = 400000
downstream_variable = 'dxctechnology_ftp_time_export_v3_send_downstream'
