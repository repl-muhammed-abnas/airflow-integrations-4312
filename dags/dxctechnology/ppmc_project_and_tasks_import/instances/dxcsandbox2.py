# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.ppmc_project_and_tasks_import.config import *

instance = 'dxcsandbox2'
environment = 'pre-production'
company_key = 'dxcsandbox2'
replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntCompass'
pgp_conn_id = 'pgp_dxctechnology_ppmc_import'
sftp_conn_id = 'sftp_dxcsandbox2_ppmc'
input_filepath = '/Test/Inbound/PPMC/Processing'
archive_filepath = '/Test/Inbound/PPMC/Archive'
log_filepath = '/Test/Inbound/PPMC/Logs'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

move_file_input_filepath = '/Test/Inbound/PPMC/Input'
move_file_process_filepath = '/Test/Inbound/PPMC/Processing'
move_file_archive_filepath = '/Test/Inbound/PPMC/Archive'
