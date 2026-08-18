# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.timedata_export.config import *

instance = "trial"


company_key = "bearingpointsandbox"

replicon_conn_id = "bearingpointsandbox_replicon_admin"
sftp_conn_id = "sftp_useast2"
http_conn_id = f'bearingpointsandbox_timedata_http_conn_{instance}'

h4s4_endpoint = '/http/timedata_H4S4_uat'
s4hc_endpoint = '/http/timedata_S4HC_uat'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

time_export_process_export_dag_id = f"bearingpoint_time_export_master_{instance}"
time_export_post_export_dag_id = f"bearingpoint_time_export_process_post_data_to_endpoint_{instance}"
time_export_post_to_s4hc_dag_id = f"bearingpoint_time_export_process_post_data_to_s4hc_{instance}"
time_export_post_to_h4s4_dag_id = f"bearingpoint_time_export_process_post_data_to_h4s4_{instance}"

timeexport_upload_backup_filepath = "/bearingpoint/time_data_export/backup"

client_id_secret_variable_name = f"bearingpoint_client_id_secret_variable_{instance}" # bearingpoint_client_id_secret_variable_trial
can_run_batch_task_var_name = f"bearingpoint_can_run_batch_task_variable_{instance}"
token_var = f"bearingpoint_token_variable_{instance}" #bearingpoint_token_variable_trial

disabled=True
