from alvarezandmarsalholdings.time_export.time_export_workday.config import *
from alvarezandmarsalholdings.time_export.time_export_workday.mappers.time_entry_code_reference_mapper import time_entry_code_reference_mapper

instance = "UAT"

company_key = "AlvarezandMarsalHoldingsUAT"

replicon_conn_id = "alvarezandmarsalholdingsuat_replicon_radmin1"
sftp_conn_id = "sftp_alvarezandmarsalholdingsuat_621229"

http_conn_id = f'alvarezandmarsalholdings_timeblock_export_http_conn_{instance}'

tenant_email = 'ITERP@alvarezandmarsal.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

time_export_to_workday_dag_id = f"AlvarezandMarsalHoldings_time_export_workday_child_dag_{instance}"

can_post_to_api_endpoint = f"AlvarezandMarsalHoldings_time_export_workday_can_post_to_api_endpoint_{instance}"

timeexport_upload_backup_filepath = "/UAT/Time Block to Workday/Output File"

TIME_ENTRY_CODE_REFERENCE_MAPPER = time_entry_code_reference_mapper
