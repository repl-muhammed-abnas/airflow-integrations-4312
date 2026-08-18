# pylint: disable=wildcard-import unused-wildcard-import
"""
ViaPlus User Sync - Production Instance Configuration
"""
from viaplus.user_sync.config import *

instance = "prod"
environment = "production"

dagid_suffix = f"_{instance}"

# Company and Connection Configuration
company_key = "ViaPlusLLC"
replicon_conn_id = "viaplusllc_replicon_adminr"

# Keka API Connections (stored in Airflow connections)
# keka_login_conn_id: Points to https://login.keka.com (for OAuth2 token endpoint)
# keka_api_conn_id: Points to https://viaplus.keka.com/api/v1 (for API calls)
keka_login_conn_id = f"viaplus_usersync_keka_login_{instance}"
keka_api_conn_id = f"viaplus_usersync_keka_api_{instance}"

keka_conn_variables = f"viaplus_user_sync_client_details_{instance}"

# Email Configuration
tenant_email = 'pc-india@viaplus.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# DAG IDs - matching CRL naming pattern
master_dagid = f"viaplus_user_sync_master{dagid_suffix}"
process_users_dagid = f"viaplus_user_sync_process_users_child{dagid_suffix}"
process_supervisor_dagid = f"viaplus_user_sync_process_supervisor_child{dagid_suffix}"
process_log_generation_dagid = f"viaplus_user_sync_process_log_generation_child{dagid_suffix}"

process_groups_dagid = f"viaplus_user_sync_process_groups_child{dagid_suffix}"
process_new_locations_dagid = f"viaplus_user_sync_process_new_location_child{dagid_suffix}"
process_new_departments_dagid = f"viaplus_user_sync_process_new_department_child{dagid_suffix}"
process_new_legal_entities_dagid = f"viaplus_user_sync_process_new_legal_entity_child{dagid_suffix}"

process_new_users_dagid = f"viaplus_user_sync_process_new_users_child{dagid_suffix}"
process_update_users_dagid = f"viaplus_user_sync_process_update_users_child{dagid_suffix}"
process_disable_users_dagid = f"viaplus_user_sync_process_disable_users_child{dagid_suffix}"

disable_future_enddate_user_master_dagid = f"viaplus_user_sync_disable_future_enddate_user_master{dagid_suffix}"
disable_future_enddate_user_child_dagid = f"viaplus_user_sync_disable_future_enddate_user_child{dagid_suffix}"

# Airflow Variable names
keka_access_token_var = f"viaplus_keka_access_token_{instance}"
keka_refresh_token_var = f"viaplus_keka_refresh_token_{instance}"
last_sync_time_var = f"viaplus_user_sync_last_sync_time_{instance}"
can_run_batch_task_var_name = f"viaplus_user_sync_run_batch_task_{instance}"

# Schedule (hourly as per spec)
schedule_interval = "@hourly"
