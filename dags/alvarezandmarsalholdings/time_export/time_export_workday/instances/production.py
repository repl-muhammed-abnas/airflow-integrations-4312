from alvarezandmarsalholdings.time_export.time_export_workday.config import *
from alvarezandmarsalholdings.time_export.time_export_workday.mappers.time_entry_code_reference_mapper import time_entry_code_reference_mapper

region = 'us-east-1'
environment = 'production'
instance = "production"

company_key = "alvarezandmarsal"

replicon_conn_id = "alvarezandmarsal_replicon_repliconint.exports"
sftp_conn_id = "sftp_alvarezandmarsal_621229"

http_conn_id = f'alvarezandmarsalholdings_timeblock_export_http_conn_{instance}'

tenant_email = 'ITERP@alvarezandmarsal.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_log_email }}'

time_export_to_workday_dag_id = f"AlvarezandMarsalHoldings_time_export_workday_child_dag_{instance}"

can_post_to_api_endpoint = f"AlvarezandMarsalHoldings_time_export_workday_can_post_to_api_endpoint_{instance}"

timeexport_upload_backup_filepath = "/Production/Time Block to Workday/Output File"

TIME_ENTRY_CODE_REFERENCE_MAPPER = time_entry_code_reference_mapper
