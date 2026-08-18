# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_wbs_import_file_merger.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
sftp_conn_id = "sftp_useast2"

input_filepath = '/Test/Inbound/GSAPWBS/Input'
processing_filepath = '/Test/Inbound/GSAPWBS/Processing'
archive_filepath = '/Test/Inbound/GSAPWBS/Archive'
log_filepath = '/Test/Inbound/GSAPWBS/Logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disable=True

disabled=True
