# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_wbs_import_file_merger.config import *

instance = 'sandbox2'
environment = 'pre-production'

company_key = 'dxcsandbox2'

replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntGSAP'
sftp_conn_id = "sftp_dxcsandbox2_gsap"

input_filepath = '/Inbound/WBS/Input'
processing_filepath = '/Inbound/WBS/Processing'
archive_filepath = '/Inbound/WBS/Archives'
log_filepath = '/Inbound/WBS/Logs'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
