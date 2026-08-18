# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.time_export_global_v4_file_based.config import *

instance = 'production'
environment = 'production'

company_key = 'capgemini'

replicon_conn_id = 'capgemini_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
sftp_conn_id_internal = 'rsftp-useast_for_testing'
pgp_conn_id = 'pgp_capgemini'

exports_data_filepath = "/Capgemini/Timedata/Input"
exports_data_archive_filepath = "/Capgemini/Timedata/Archive"
input_filepath = "/Outbound/GlobalTimedata/Input"
log_filepath = "/Outbound/GlobalTimedata/Logs"
s3_upload_filepath = "Capgemini/Outbound/GlobalTimedata/Input"

time_export_file_format = 'Global Data Hub Extract'
excepted_export_locations = "Mexico"
timesheet_period_base_user_location = "India"

tenant_email = 'groupitrepliconsupportl2@capgemini.com,gtminterfacenotifications.hr@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},capgeminisupportreplicon@deltek.com'

master_dag_id = f'capgemini_time_export_global_file_based_master_{instance}'
time_export_child_dag_id = f'capgemini_time_export_global_file_based_download_export_child_{instance}'

export_file_prefix = "Prod"
can_send_time_export_downstream = f'capgemini_time_export_global_file_based_send_downstream_{instance}'
can_run_batch_task_var_name = f'capgemini_time_export_global_file_based_can_run_batch_task_{instance}'
