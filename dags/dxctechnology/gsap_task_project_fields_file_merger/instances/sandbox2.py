# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_task_project_fields_file_merger.config import *

instance = 'sandbox2'
environment = 'pre-production'

company_key = 'dxcsandbox2'

replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntGSAP'
sftp_conn_id = "sftp_dxcsandbox2_gsap"

input_filepath = '/Inbound/Tasks/Input'
processing_filepath = '/Inbound/Tasks/Processing'
archive_filepath = '/Inbound/Tasks/Archives'
log_filepath = '/Inbound/Tasks/Logs/FilemergeLog'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
