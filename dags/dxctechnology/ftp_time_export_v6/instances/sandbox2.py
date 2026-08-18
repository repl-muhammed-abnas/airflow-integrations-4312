# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.ftp_time_export_v6.config import *
from dxctechnology.ftp_time_export_v6.mappers.timeoff_types_mapper import timeoff_types_to_exclude
from dxctechnology.ftp_time_export_v6.mappers.timetype_standby_units_mapper import timetype_standby_units_to_exclude

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
downstream_variable = f'dxctechnology_ftp_time_export_v6_send_downstream_{instance}'

timeoff_types_to_exclude_mapper = timeoff_types_to_exclude
timetype_standby_units_to_exclude_mapper = timetype_standby_units_to_exclude
