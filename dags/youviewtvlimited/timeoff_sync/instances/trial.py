# pylint: disable=wildcard-import unused-wildcard-import
from youviewtvlimited.timeoff_sync.config import *

instance = "trial"
environment = "pre-production"
company_key = "youviewtvlimitedtrial01"
replicon_conn_id = "youviewtvlimitedtrial01_replicon_admin"
http_conn_id = f"youviewtv_timeoff_sync_http_{instance}"
http_conn_id_search_employee = f"youviewtv_timeoff_sync_search_employee_http_{instance}"

tenant_email = 'repliconmailbox@youview.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

access_token = f"youviewtv_timeoff_sync_access_token_{instance}"

master_dag = f'youviewtv_timeoff_sync_master_{instance}_v1'
timeoff_booking_child = f"youviewtv_timeoff_sync_timeoff_booking_child_{instance}_v1"
timeoff_delete_child = f"youviewtv_timeoff_sync_timeoff_delete_child_{instance}_v1"

TIMEOFF_TYPE_NO_SYNC = ['Working away']

can_run_batch_task_var_name = "youviewtv_timeoff_sync_booking_child_can_run_batch_task"
lookup_log_timestamp_var = f'youviewtv_timeoff_sync_lookup_log_timestamp_{instance}'

disabled=True
