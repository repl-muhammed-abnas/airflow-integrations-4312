# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_wbs_file_merger.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = "dxcsandbox"
company_key = 'dxcsandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntCompass'
sftp_conn_id = 'dxcsandbox-sftp-628172_Compass'
sub_erp = 'NT2'
input_filepath = '/Test/Inbound/COMPASSWBSMaster/NT2/Input'
log_filepath = '/Test/Inbound/COMPASSWBSMaster/NT2/Logs/FilemergeLog'
processing_file_directory = '/Test/Inbound/COMPASSWBSMaster/NT2/Processing'
archive_filepath = '/Test/Inbound/COMPASSWBSMaster/NT2/Archive'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
dag_id_postfix = f'{instance}_{sub_erp}'
