# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_billing_key_file_merger.config import *

instance = 'production'
environment = 'production'

company_key = 'dxctechnology'

replicon_conn_id = 'dxctechnology_replicon_RepliconIntGSAP'
sftp_conn_id = "sftp_dxctechnology_gsap"

input_filepath = '/Inbound/Billing Key/Input'
processing_filepath = '/Inbound/Billing Key/Processing'
archive_filepath = '/Inbound/Billing Key/Archives'
log_filepath = '/Inbound/Billing Key/Logs'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
