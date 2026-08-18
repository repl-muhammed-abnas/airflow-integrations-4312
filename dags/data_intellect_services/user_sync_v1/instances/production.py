# pylint: disable=wildcard-import unused-wildcard-import
from data_intellect_services.user_sync_v1.config import *
from data_intellect_services.user_sync_v1.mapper.non_readable_columns import fields

instance = "production"
environment = "production"
company_key = "dataintellect"
replicon_conn_id = "dataintellect_replicon_admin"

http_conn_id = f"data_intellect_user_sync_http_{instance}"
access_token = f"data_intellect_user_sync_access_token_{instance}"

tenant_email = "david.richardson@dataintellect.com,connor.metcalf@dataintellect.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

tenant_wide_log_name_for_logs = f"data_intellect_user_sync_tenant_wide_log_for_logs_{instance}_v1"
user_sync_tenant_wide_log_name = f"data_intellect_user_sync_tenant_wide_log_{instance}_v1"

can_run_batch_task_scheduled_logs_var_name = f"data_intellect_user_sync_scheduled_logs_can_run_batch_{instance}_v1"
can_run_batch_create_user_child_var_name = f"data_intellect_user_sync_create_user_child_can_run_batch_{instance}_v1"
can_run_batch_update_user_child_var_name = f"data_intellect_user_sync_update_user_child_can_run_batch_{instance}_v1"
can_run_batch_process_users_child_var_name = f"data_intellect_user_sync_process_users_child_can_run_batch_{instance}_v1"
can_run_batch_user_sync_master_var_name = f"data_intellect_user_sync_master_can_run_batch_{instance}_v1"

non_readable_columns = fields
