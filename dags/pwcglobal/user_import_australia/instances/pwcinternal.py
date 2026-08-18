# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.user_import_australia.config import *

instance = "pwcinternal"

replicon_conn_id = 'pwcinternal-replicon-eu.automation'
sftp_conn_id = 'sftp_pwc_userimport'

user_import_input_path = "/PwCGBL_Replicon_Internal/Australia/Outbound/User data"
user_import_archive_path = "/PwCGBL_Replicon_Internal/Australia/Outbound/User data/Archive/"
user_import_log_path = "/PwCGBL_Replicon_Internal/Australia/Outbound/User data/Logs/"

user_allowance_input_path = "/PwCGBL_Replicon_Internal/Australia/Outbound/Allowances"
user_allowance_log_path = "/PwCGBL_Replicon_Internal/Australia/Outbound/Allowances/Logs/"
user_allowance_archive_path = "/PwCGBL_Replicon_Internal/Australia/Outbound/Allowances/Archive/"

termination_details_input_path = "/PwCGBL_Replicon_Internal/Australia/Outbound/Termination details/"
termination_details_log_filepath = '/PwCGBL_Replicon_Internal/Australia/Outbound/Termination details/Logs/'
termination_details_archive_path = '/PwCGBL_Replicon_Internal/Australia/Outbound/Termination details/Archive/'

reference_file = "/usersync/reference/pwcglobal_aus_userImport_reference_file.csv"
reference_archive_file_path = "/usersync/Archives/"
reference_sftp_conn_id = "sftp_pwc_userimport"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True
