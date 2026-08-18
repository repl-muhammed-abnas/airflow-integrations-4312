from alvarezandmarsalholdings.time_export.timeoff_export_workday.config import *

instance = "UAT"

company_key = "AlvarezandMarsalHoldingsUAT"

replicon_conn_id = "alvarezandmarsalholdingsuat_replicon_radmin1"
sftp_conn_id = "sftp_alvarezandmarsalholdingsuat_621229"

http_conn_id_timeoff_workday = f'alvarezandmarsalholdings_timeoff_export_http_conn_{instance}'

tenant_email = 'ITERP@alvarezandmarsal.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

timeoff_export_to_workday_dag_id = f"AlvarezandMarsalHoldings_timeoff_export_workday_child_dag_{instance}"

can_post_to_api_endpoint = f"AlvarezandMarsalHoldings_timeoff_export_workday_can_post_to_api_endpoint_{instance}"

timeoff_export_upload_backup_filepath = "/UAT/Time Off to Workday/Output File"
