# pylint: disable=wildcard-import unused-wildcard-import
from youviewtvlimited.timeoff_sync_v2.config import *
from youviewtvlimited.timeoff_sync_v2.mapper.timeoff_type_mapping import HIBOB_TO_REPLICON_TIMEOFF_TYPE

instance = "prod"
environment = "production"
company_key = "YouViewTVLimited"
replicon_conn_id = "YouViewTVLimited_replicon_Repliconint.timeoffsync"
http_conn_id = f"youviewtv_timeoff_sync_http_{instance}"

tenant_email = "repliconmailbox@youview.com"
internal_logs_email = "{{ var.value.dagrun_internal_log_email }}"
alert_email = "{{ var.value.dagrun_failure_alert_email }}"

version = "v2"

master_dag = f"youviewtvlimited_timeoff_sync_master_{instance}_{version}"
timeoff_booking_child = f"youviewtvlimited_timeoff_sync_timeoff_booking_child_{instance}_{version}"
timeoff_delete_child = f"youviewtvlimited_timeoff_sync_timeoff_delete_child_{instance}_{version}"

HIBOB_TO_REPLICON_TIMEOFF_TYPES_MAPPER = HIBOB_TO_REPLICON_TIMEOFF_TYPE

can_run_batch_task_var_name = "youviewtv_timeoff_sync_booking_child_can_run_batch_task"
lookup_log_timestamp_var = f"youviewtv_timeoff_sync_lookup_log_timestamp_{instance}"
