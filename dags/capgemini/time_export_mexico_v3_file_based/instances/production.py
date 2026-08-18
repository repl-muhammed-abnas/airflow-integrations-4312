# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.time_export_mexico_v3_file_based.config import *

instance = 'production'
environment = 'production'

company_key = 'capgemini'

replicon_conn_id = 'capgemini_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
sftp_conn_id_internal = 'rsftp-useast_for_testing'
pgp_conn_id = 'pgp_capgemini'

exports_data_filepath = "/Capgemini/MexicoTimedata/Input"
exports_data_archive_filepath = "/Capgemini/MexicoTimedata/Archive"
input_filepath = "/Outbound/Timedata/Input"
log_filepath = "/Outbound/Timedata/Logs"
s3_upload_filepath = "Capgemini/Outbound/Timedata/Input"

timeoff_types_task_codes_mapper = "capgemini_time_export_timeoff_types_task_codes_mapper"

export_locations = "Mexico"
export_start_date = "2023/07/01"
time_export_file_format = 'GFS Extract Mexico'

tenant_email = 'groupitrepliconsupportl2@capgemini.com,gtminterfacenotifications.hr@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},capgeminisupportreplicon@deltek.com'
can_send_time_export_downstream = f"capgemini_time_export_mexico_file_based_send_downstream_{instance}"
master_dag_id = f'capgemini_time_export_mexico_file_based_master_{instance}'
time_export_child_dag_id = f'capgemini_time_export_mexico_file_based_download_export_child_{instance}'
can_run_batch_task_var_name = f'capgemini_time_export_mexico_file_based_can_run_batch_task_{instance}'
