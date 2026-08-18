# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.user_import_australia.config import *

instance = "production"
environment = 'production'

company_key = "pwc"

replicon_conn_id = 'pwcglobal-replicon-admin.australia'
sftp_conn_id = 'pwcglobal-AUS-MFT-PRD-replicon'

user_import_input_path = "/PwCGBL_Replicon_PRD/Australia/Outbound/User data"
user_import_archive_path = "/PwCGBL_Replicon_PRD/Australia/Outbound/User data/Archive/"
user_import_log_path = "/PwCGBL_Replicon_PRD/Australia/Outbound/User data/Logs/"

user_allowance_input_path = " /PwCGBL_Replicon_PRD/Australia/Outbound/Allowances"
user_allowance_archive_path = "/PwCGBL_Replicon_PRD/Australia/Outbound/Allowances/Archive/"
user_allowance_log_path = "/PwCGBL_Replicon_PRD/Australia/Outbound/Allowances/Logs/"

termination_details_input_path = "/PwCGBL_Replicon_PRD/Australia/Outbound/Termination details"
termination_details_log_filepath = '/PwCGBL_Replicon_PRD/Australia/Outbound/Termination details/Logs/'
termination_details_archive_path = '/PwCGBL_Replicon_PRD/Australia/Outbound/Termination details/Archive/'

reference_file = "/usersync/reference/pwcglobal_aus_userImport_reference_file.csv"
reference_archive_file_path = "/usersync/Archives/"
reference_sftp_conn_id = "pwc-internal-PRD-replicon"

tenant_email = 'au_repliconadmin@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

add_user_max_active_runs = 4
update_user_max_active_runs = 4
child_max_active_runs = 4
max_active_runs_supervisor = 2
