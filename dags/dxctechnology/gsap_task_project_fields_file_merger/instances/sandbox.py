# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_task_project_fields_file_merger.config import *

instance = 'sandbox'
environment = 'pre-production'

company_key = 'dxcsandbox'

replicon_conn_id = 'dxcsandbox-replicon-RepliconIntGSAP'
sftp_conn_id = "sftp_dxctechnology_gsap"

input_filepath = '/Inbound/Tasks/Input'
processing_filepath = '/Inbound/Tasks/Processing'
archive_filepath = '/Inbound/Tasks/Archives'
log_filepath = '/Inbound/Tasks/Logs/FilemergeLog'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
