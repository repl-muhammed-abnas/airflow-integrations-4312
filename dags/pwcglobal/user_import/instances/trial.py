# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.user_import.config import *

instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'PwCinternal'
replicon_conn_id = 'replicon_pwcglobal'
sftp_conn_id = "sftp_pwc_userimport"

input_filepath = "/PwCGlobal/User_Import/input"
archive_filepath = "/PwCGlobal/User_Import/archive"
log_filepath = "/PwCGlobal/User_Import/logs"

report_process_size = 5  # only for QA Testing
disabled = True
