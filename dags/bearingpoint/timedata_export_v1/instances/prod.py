# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.timedata_export_v1.config import *

instance = "prod"
environment = "production"
company_key = "bearingpointgmbh"

replicon_conn_id = "bearingpointgmbh_replicon_repliconint.time_export"
# For uat/prod sftp connection is not required, this connection is kept because it is used in post_h4s4/s4hc file and trial instance.
sftp_conn_id = "sftp_useast2" 
http_conn_id = f'bearingpointgmbh_timedata_http_conn_{instance}'

h4s4_endpoint = '/http/timedata_H4S4_prod'
s4hc_endpoint = '/http/timedata_S4HC_prod'

tenant_email = 'work.smtp-0125104@bearingpoint.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_log_email }}'

version = 'v1'

time_export_process_export_dag_id = f"bearingpoint_time_export_master_{instance}_{version}"
time_export_post_export_dag_id = f"bearingpoint_time_export_process_post_data_to_endpoint_{instance}_{version}"
time_export_post_to_s4hc_dag_id = f"bearingpoint_time_export_process_post_data_to_s4hc_{instance}_{version}"
time_export_post_to_h4s4_dag_id = f"bearingpoint_time_export_process_post_data_to_h4s4_{instance}_{version}"

timeexport_upload_backup_filepath = "/bearingpoint/time_data_export/backup"

client_id_secret_variable_name = f"bearingpoint_client_id_secret_variable_{instance}" #bearingpoint_client_id_secret_variable_prod
can_run_batch_task_var_name = f"bearingpoint_can_run_batch_task_variable_{instance}" #bearingpoint_can_run_batch_task_variable_prod
token_var = f"bearingpoint_token_variable_{instance}" #bearingpoint_token_variable_prod
