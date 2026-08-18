# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_wbs_file_merger_v2.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = "dxcsandbox2"
sub_erp = 'NT4'
company_key = 'dxcsandbox2'
replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntCompass'
sftp_conn_id = 'dxcsandbox2-sftp-628172_Compass'
input_filepath = '/Test/Inbound/COMPASSWBSMaster/NT4/Input'
log_filepath = '/Test/Inbound/COMPASSWBSMaster/NT4/Logs/FilemergeLog'
processing_file_directory = '/Test/Inbound/COMPASSWBSMaster/NT4/Processing'
archive_filepath = '/Test/Inbound/COMPASSWBSMaster/NT4/Archive'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
dag_id_postfix = f'{instance}_{sub_erp}'
