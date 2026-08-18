# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.ppmc_project_and_tasks_import.config import *

instance = 'DXCTechnology'
region = 'us-east-2'
environment = 'production'
company_key = 'DXCTechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntCompass'
pgp_conn_id = 'pgp_dxctechnology_ppmc_import'
sftp_conn_id = 'sftp_dxctechnology_ppmc'
input_filepath = '/Production/Inbound/PPMC/Processing'
archive_filepath = '/Production/Inbound/PPMC/Archive'
log_filepath = '/Production/Inbound/PPMC/Logs'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = ''
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

move_file_input_filepath = '/Production/Inbound/PPMC/Input'
move_file_process_filepath = '/Production/Inbound/PPMC/Processing'
move_file_archive_filepath = '/Production/Inbound/PPMC/Archive'
