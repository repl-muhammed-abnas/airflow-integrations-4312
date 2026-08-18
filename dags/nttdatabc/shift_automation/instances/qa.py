# pylint: disable=wildcard-import unused-wildcard-import
from nttdatabc.shift_automation.config import *

instance = 'qa'
environment = 'pre-production'

company_key = 'nttdatabctrial02'
replicon_conn_id = 'replicon-nttdatabctrial-admin'
sftp_conn_id = 'sftp_nttdatabc_656377'

output_reference_file_path = "NTTDataBCTrial/shiftautomation/reference/newreference.csv"
archive_file_path = "NTTDataBCTrial/shiftautomation/archive"

log_filepath = "/Trial/Shift schedule Log file"

tenant_email = 'kathy.reeves@nttdata.com,mohandas.kalathil@nttdata.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
disabled = True
