# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.ftp_time_export_pta_v2.config import *

region = 'us-east-2'
environment = 'production'
instance = "production"
company_key = 'dxctechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntFTP'
sumo_conn_id = 'sumologic-exportlogger'
sftp_conn_id = 'dxctechnology-sftp-628172_FTP'
http_conn_id = 'dxctechnology-dxc-REPLICON_POP'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
exception_email = '{{ var.value.dagrun_failure_alert_email }}'
sftp_upload_path = '/Production/Outbound/FTPTimeExtract'
row_threshold = 400000
execution_timeout_days = 14
downstream_variable = 'dxctechnology_ftp_time_export_pta_v2_send_downstream'
alert_email = 'xtssap@dxc.com'
