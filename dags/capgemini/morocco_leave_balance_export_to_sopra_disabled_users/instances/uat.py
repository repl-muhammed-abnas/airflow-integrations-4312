# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.morocco_leave_balance_export_to_sopra_disabled_users.config import *
from capgemini.morocco_leave_balance_export_to_sopra_disabled_users.mapper.timeoff_codes import timeoff_codes_list

instance = 'uat'
location = 'Morocco'

environment = 'pre-production'

company_key = 'capgeminiuat'

replicon_conn_id = 'capgeminiuat_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_sopra_morocco_capgeminiuat'

input_filepath = "/Outbound/MAR_TOBalance_ZYWV/Input"
s3_upload_filepath = "CapgeminiUAT/Outbound/MAR_TOBalance_ZYWV/Input"
ma01_filename_prefix = "ZYWV_UAT_replicon_MA01"
ma02_ma03_filename_prefix = "ZYWV_UAT_replicon_MA02_MA03"

leave_balance_report_name = "Morocco ZYWV Leave Balances - Disabled Users"
timeoff_paycodes = timeoff_codes_list

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_morocco_leave_balance_extract_disabled_users_can_run_batch_task_{instance}'

master_dag_id = f'capgemini_morocco_leave_balance_extract_to_sopra_disabled_users_master_{instance}'
