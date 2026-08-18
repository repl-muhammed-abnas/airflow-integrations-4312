# pylint: disable=wildcard-import unused-wildcard-import
from youviewtvlimited.timeoff_sync.config import *

instance = "prod"
environment = "production"
company_key = "YouViewTVLimited"
replicon_conn_id = "YouViewTVLimited_replicon_Repliconint.timeoffsync"
http_conn_id = f"youviewtv_timeoff_sync_http_{instance}"


tenant_email = "repliconmailbox@youview.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag = f'youviewtv_timeoff_sync_master_{instance}_v1'
timeoff_booking_child = f"youviewtv_timeoff_sync_timeoff_booking_child_{instance}_v1"
timeoff_delete_child = f"youviewtv_timeoff_sync_timeoff_delete_child_{instance}_v1"

TIMEOFF_TYPE_NO_SYNC = ['Working away']

can_run_batch_task_var_name = "youviewtv_timeoff_sync_booking_child_can_run_batch_task"
lookup_log_timestamp_var = f'youviewtv_timeoff_sync_lookup_log_timestamp_{instance}'
