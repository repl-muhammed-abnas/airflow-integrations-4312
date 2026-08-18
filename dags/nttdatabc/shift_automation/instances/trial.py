# pylint: disable=wildcard-import unused-wildcard-import
from nttdatabc.shift_automation.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'nttdatabctrial02'
replicon_conn_id = 'replicon-nttdatabctrial-admin'
sftp_conn_id = 'rsftp-useast_for_testing'

output_reference_file_path = "NTTDataBCTrial/shiftautomation/reference/newreference.csv"
archive_file_path = "NTTDataBCTrial/shiftautomation/archive"

log_filepath = "/Trial/Shift schedule Log file"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
disabled = True
