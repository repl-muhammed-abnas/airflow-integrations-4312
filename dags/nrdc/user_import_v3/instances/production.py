# pylint: disable=wildcard-import unused-wildcard-import
from nrdc.user_import_v3.config import *

environment = 'production'

instance = "production"

company_key = 'NRDC'
replicon_conn_id = 'nrdc_replicon_admin'
can_run_batch_task_var_name = f'nrdc_user_import_usa_can_run_batch_task_{instance}'
user_report_name = '**User List For Email Notification**'
sftp_conn_id = "sftp_nrdc_639645"
sftp_conn_id2 = "sftp_gmailToSFTP_Integration_GmailtoSFTP"

input_filepath = "/NRDC/nrdc.userimport/Input"
archive_filepath = "/NRDC/nrdc.userimport/Archive"

log_filepath = "/Logs"

tenant_email = "replicon.accountissues@nrdc.org"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
user_import_report_name = '***User Import Reference'

version = "_v3"

nrdc_userimport_master = f"nrdc_user_import_master_{instance}{version}"
nrdc_updating_c3_c4_values = f"nrdc_user_import_process_update_user_child_{instance}{version}"
nrdc_updaterehiredisableuserbasicprofile = f"nrdc_user_import_process_rehire_user_child_{instance}{version}"
nrdc_basicaddupdate = f"nrdc_user_import_process_create_new_user_profile_child_{instance}{version}"
nrdc_assignsubstituteusersv2 = f"nrdc_user_import_process_substitute_users_child_{instance}{version}"
nrdc_add_user_v2 = f"nrdc_user_import_process_add_user_child_{instance}{version}"

c3_c4_profile_supervisors_variable = f"user_import_c3_c4_supervisors_{instance}"
