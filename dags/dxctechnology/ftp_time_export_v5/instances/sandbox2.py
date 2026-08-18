# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.ftp_time_export_v5.config import *

instance = "sandbox2"
company_key = 'DXCSandbox2'
replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntFTP'
sumo_conn_id = 'sumologic-exportlogger'
sftp_conn_id = 'dxcsandbox2-sftp-628172_FTP'
http_conn_id = 'dxcsandbox2-dxc-REPLICON_POQ'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
exception_email = '{{ var.value.dagrun_failure_alert_email }}'
alert_email = 'xtssap@dxc.com'
sftp_upload_path = '/Test/Outbound/FTPTimeExtract'
row_threshold = 400000
execution_timeout_days = 14
downstream_variable = f'dxctechnology_ftp_time_export_v5_send_downstream_{instance}'
