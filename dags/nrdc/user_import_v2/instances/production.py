# pylint: disable=wildcard-import unused-wildcard-import
from nrdc.user_import_v2.config import *

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

nrdc_userimport_master = f"nrdc_userimport_master_{instance}_v2"
nrdc_updating_c3_c4_values = f"nrdc_userimport_updating_c3_c4_values_child_{instance}_v2"
nrdc_updaterehiredisableuserbasicprofile = f"nrdc_userimport_updaterehiredisableuserbasicprofile_child_{instance}_v2"
nrdc_basicaddupdate = f"nrdc_userimport_basicaddupdate_child_{instance}_v2"
nrdc_assignsubstituteusersv2 = f"nrdc_userimport_assignsubstituteusers_child_{instance}_v2"
nrdc_add_user_v2 = f"nrdc_userimport_add_user_child_{instance}_v2"

c3_c4_profile_supervisors_variable = f"user_import_c3_c4_supervisors_{instance}"


