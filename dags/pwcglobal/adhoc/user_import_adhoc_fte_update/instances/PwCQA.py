# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.adhoc.user_import_adhoc_fte_update.config import *

instance = 'PwCQA'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'PwCQA'
replicon_conn_id = 'pwcqa-replicon-eu.userimport'
sftp_conn_id = "sftp_useast2"
keynamespace="FTE_Value"
input_filepath = "/PWC/PwCQA/fte_adhoc_user_import/input"
archive_filepath = "/PWC/PwCQA/fte_adhoc_user_import/archive/"
user_import_fte_adhoc_run_master=f"pwcglobal_user_import_adhoc_fte_initial_update_master_{instance}"
user_fte_update_child=f"pwcglobal_user_import_adhoc_fte_initial_update_child_{instance}"
