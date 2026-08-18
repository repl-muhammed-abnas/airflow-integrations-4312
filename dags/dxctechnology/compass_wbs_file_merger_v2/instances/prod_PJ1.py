# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_wbs_file_merger_v2.config import *

region = 'us-east-2'
environment = 'production'
instance = 'prod'
sub_erp = 'PJ1'
company_key = 'dxctechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntCompass'
sftp_conn_id = 'DXCTechnology-sftp-628172_COMPASS'
input_filepath = '/Production/Inbound/COMPASSWBSMaster/PJ1/Input'
log_filepath = '/Production/Inbound/COMPASSWBSMaster/PJ1/Logs/FilemergeLog'
processing_file_directory = '/Production/Inbound/COMPASSWBSMaster/PJ1/Processing'
archive_filepath = '/Production/Inbound/COMPASSWBSMaster/PJ1/Archive'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
dag_id_postfix = f'{instance}_{sub_erp}'
