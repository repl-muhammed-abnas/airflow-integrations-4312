# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_billing_key_file_merger.config import *

instance = 'sandbox2'
environment = 'pre-production'

company_key = 'dxcsandbox2'

replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntGSAP'
sftp_conn_id = "sftp_dxcsandbox2_gsap"

input_filepath = '/Inbound/Billing Key/Input'
processing_filepath = '/Inbound/Billing Key/Processing'
archive_filepath = '/Inbound/Billing Key/Archives'
log_filepath = '/Inbound/Billing Key/Logs'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
