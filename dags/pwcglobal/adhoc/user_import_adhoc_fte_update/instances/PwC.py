# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.adhoc.user_import_adhoc_fte_update.config import *

instance = 'prod'
region = 'eu-central-1'
environment = 'production'

company_key = 'PwC'
replicon_conn_id = 'pwcglobal-replicon-eu.userimport'
sftp_conn_id = "pwc-internal-PRD-replicon"
keynamespace="FTE_Value"
input_filepath = "/PWC/PwCPRD/fte_adhoc_user_import/input"
archive_filepath = "/PWC/PwCPRD/fte_adhoc_user_import/archive/"
user_import_fte_adhoc_run_master=f"pwcglobal_user_import_adhoc_fte_initial_update_master_{instance}"
user_fte_update_child=f"pwcglobal_user_import_adhoc_fte_initial_update_child_{instance}"
