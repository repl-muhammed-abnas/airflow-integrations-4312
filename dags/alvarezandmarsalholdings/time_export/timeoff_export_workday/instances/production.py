from alvarezandmarsalholdings.time_export.timeoff_export_workday.config import *

region = 'us-east-1'
environment = 'production'
instance = "production"

company_key = "alvarezandmarsal"

replicon_conn_id = "alvarezandmarsal_replicon_repliconint.exports"
sftp_conn_id = "sftp_alvarezandmarsal_621229"

http_conn_id_timeoff_workday = f'alvarezandmarsalholdings_timeoff_export_http_conn_{instance}'

tenant_email = 'ITERP@alvarezandmarsal.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_log_email }}'

timeoff_export_to_workday_dag_id = f"AlvarezandMarsalHoldings_timeoff_export_workday_child_dag_{instance}"

can_post_to_api_endpoint = f"AlvarezandMarsalHoldings_timeoff_export_workday_can_post_to_api_endpoint_{instance}"

timeoff_export_upload_backup_filepath = "/Production/Time Off to Workday/Output File"
