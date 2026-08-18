# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.time_export_v2.config import *

instance = "prod"

environment = "production"

company_key = "mammoet"

replicon_conn_id = "mammoet_replicon_admin"
sftp_conn_id = "sftp_mammoet_550793"
http_conn_id = f'mammoet_timedata_export_http_conn_{instance}'

tenant_email = 'RepliconNotifications@mammoet.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_log_email }}'

time_export_process_missing_replicon_ids_master_dag_id = f"mammoet_time_export_process_missing_replicon_ids_master_dag_id_{instance}_v2"
time_export_process_export_dag_id = f"mammoet_time_export_master_{instance}_v2"
time_export_process_timesheets_dag_id = f"mammoet_time_export_process_timesheets_child_{instance}_v2"
time_export_process_timesheets_time_entries_dag_id = f"mammoet_time_export_process_timesheets_time_entries_child_{instance}_v2"
time_export_post_export_dag_id = f"mammoet_time_export_process_post_data_to_endpoint_{instance}_v2"

timeexport_upload_input_filepath = "/Production/Time Data Export to S4/Input"
timeexport_upload_backup_filepath = "/Production/Time Data Export to S4/Backup"

client_id_secret_variable_name = f"mammoet_client_id_secret_variable_{instance}"

daily_run_schedule_interval = "0 */1 * * *"
