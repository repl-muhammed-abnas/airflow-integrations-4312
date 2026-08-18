from jira_time_sync_cp_polaris.config import *

instance = "prod"
environment = "production"
company_key = "RepliconPInc"

master_dag_id = f"jira_time_sync_master_{instance}"
replicon_child_dag_id = f"jira_time_sync_replicon_child_{instance}"

jira_conn_id = "deltek_jira_timesync_prod"
replicon_conn_id = "repliconpinc_replicon_integration"

jira_project_custom_field = "customfield_10062"
jira_task_custom_field = None

rep_hardcoded_project_name = "TCoE APAC - Incident Management"
rep_hardcoded_task_name = "Incident cases"
rep_hardcoded_activity_name = "Work From Home"

max_retries = 2
retry_delay_seconds = 30

sync_enabled_var_name = f"jira_time_sync_{instance}_enabled"
can_run_batch_task_var_name = f"{company_key}_{instance}_batch_task_var"

hmac_secret_jira = f"hmac_secret_jira_polaris_sync_{instance}"
