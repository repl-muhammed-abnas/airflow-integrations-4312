# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_wbs_import_file_merger.config import *

instance = 'sandbox'
environment = 'pre-production'

company_key = 'dxcsandbox'

replicon_conn_id = 'dxcsandbox-replicon-RepliconIntGSAP'
sftp_conn_id = "sftp_dxctechnology_gsap"

input_filepath = '/Inbound/WBS/Input'
processing_filepath = '/Inbound/WBS/Processing'
archive_filepath = '/Inbound/WBS/Archives'
log_filepath = '/Inbound/WBS/Logs'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
