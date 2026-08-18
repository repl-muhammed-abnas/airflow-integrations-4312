# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.time_export_v2.config import *

instance = "trial"


company_key = "mammoettrial01trial01"

replicon_conn_id = "mammoettrial01trial01_replicon_admin"
sftp_conn_id = "sftp_useast2"
http_conn_id = f'mammoettrial01trial01_timedata_http_conn_{instance}'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

time_export_process_missing_replicon_ids_master_dag_id = f"mammoet_time_export_process_missing_replicon_ids_master_dag_id_{instance}_v2"
time_export_process_export_dag_id = f"mammoet_time_export_master_{instance}_v2"
time_export_process_timesheets_dag_id = f"mammoet_time_export_process_timesheets_child_{instance}_v2"
time_export_process_timesheets_time_entries_dag_id = f"mammoet_time_export_process_timesheets_time_entries_child_{instance}_v2"
time_export_post_export_dag_id = f"mammoet_time_export_process_post_data_to_endpoint_{instance}_v2"

timeexport_upload_input_filepath = "/Time Data Export to S4/Trial01Trial01/Input"
timeexport_upload_backup_filepath = "/Time Data Export to S4/Trial01Trial01/Backup"

client_id_secret_variable_name = f"mammoet_client_id_secret_variable_{instance}"
disabled = True
