# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.ftp_time_export_pta_v4.config import *
from dxctechnology.ftp_time_export_pta_v4.mappers.timeoff_types_mapper import timeoff_types_to_exclude
from dxctechnology.ftp_time_export_pta_v4.mappers.timetype_standby_units_mapper import timetype_standby_units_to_exclude

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
downstream_variable = f'dxctechnology_ftp_time_export_pta_v4_send_downstream_{instance}'
alert_email = 'xtssap@dxc.com'

master_dag_id = f'dxctechnology_time_export_ftp_pta_v4_master_{instance}'
process_unackn_child_dag_id = f'dxctechnology_time_export_ftp_pta_v4_process_all_unackn_export_child_{instance}'

timeoff_types_to_exclude_mapper = timeoff_types_to_exclude
timetype_standby_units_to_exclude_mapper = timetype_standby_units_to_exclude
