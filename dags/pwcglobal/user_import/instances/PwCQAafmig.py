# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.user_import.config import *

instance = 'PwCQAafmig'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'PwCQAafmig'
replicon_conn_id = 'pwcqaafmig-replicon-eu.automation'
sftp_conn_id = "sftp_pwc_userimport"

input_filepath = "/PwCQAafmig/User_Import/input"
archive_filepath = "/PwCQAafmig/User_Import/archive"
log_filepath = "/PwCQAafmig/User_Import/logs"
disabled = True
