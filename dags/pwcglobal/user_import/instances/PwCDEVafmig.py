# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.user_import.config import *

instance = 'PwCDEVafmig'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'PwCDEVafmig'
replicon_conn_id = 'pwcdevafmig-replicon-eu.automation'
sftp_conn_id = "sftp_pwc_userimport"

input_filepath = "/PwCDEVafmig/User_Import/input"
archive_filepath = "/PwCDEVafmig/User_Import/archive"
log_filepath = "/PwCDEVafmig/User_Import/logs"
disabled = True
