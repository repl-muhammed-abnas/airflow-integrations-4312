# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_billing_key_file_merger.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
sftp_conn_id = "sftp_useast2"

input_filepath = '/Test/Inbound/BillingKey/Input'
processing_filepath = '/Test/Inbound/BillingKey/Processing'
archive_filepath = '/Test/Inbound/BillingKey/Archive'
log_filepath = '/Test/Inbound/BillingKey/Logs'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disable=True

disabled=True
