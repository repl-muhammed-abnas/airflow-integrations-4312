# pylint: disable=wildcard-import unused-wildcard-import
from nttdatabc.shift_automation.config import *

instance = 'production'
environment = 'production'

company_key = 'nttdatabc'
replicon_conn_id = 'nttdatabc_replicon_admin'
sftp_conn_id = 'sftp_nttdatabc_656377'

output_reference_file_path = "NTTDataBC/shiftautomation/reference/newreference.csv"
archive_file_path = "NTTDataBC/shiftautomation/archive"

log_filepath = "/Production/Shift schedule Log file"

tenant_email = 'kathy.reeves@nttdata.com,mohandas.kalathil@nttdata.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
