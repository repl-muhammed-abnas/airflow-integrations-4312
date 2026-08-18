# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.user_import_v1.config import *

instance = 'devops'
region = 'us-west-2'
environment = 'devops'

company_key = 'PwCinternal'
replicon_conn_id = 'replicon_pwcglobal'
sftp_conn_id = "sftp_pwc_userimport"

input_filepath = "/PwCGlobal/User_Import/input"
archive_filepath = "/PwCGlobal/User_Import/archive"
log_filepath = "/PwCGlobal/User_Import/logs"

report_process_size = 5  # only for QA Testing

is_secondary_upload_required = False
