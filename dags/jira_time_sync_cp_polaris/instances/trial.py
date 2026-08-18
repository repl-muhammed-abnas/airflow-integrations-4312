"""
JIRA Time Sync Integration - Trial Instance Configuration
==========================================================

Trial/testing environment configuration for JIRA to Replicon Polaris
time sync integration.

NOTE: Costpoint sync is not part of the current release — see
backup/costpoint_integration_backup.py for its config/DAG if reactivating.
"""

from jira_time_sync_cp_polaris.config import *

instance = "trial"
environment = "pre-production"
company_key = "Repliconpincstream6dev"

master_dag_id = f"jira_time_sync_master_{instance}"
replicon_child_dag_id = f"jira_time_sync_replicon_child_{instance}"

jira_conn_id = "deltek_jira_trial"
replicon_conn_id = "repliconpincstream6dev_replicon_integration"

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

# ---------------------------------------------------------------------------
# Deployment notes
# ---------------------------------------------------------------------------
# 1. Airflow Variable required before first run:
#      Key:   hmac_secret_jira_polaris_sync_trial
#      Value: the HMAC secret chosen when registering the JIRA system webhook
#      Owner: integration team — create via Airflow UI (Admin → Variables)
#             or the Airflow CLI; do NOT commit the secret value to git.
#
# 2. JIRA-side setup:
#      - Register a system webhook in JIRA (Settings → System → Webhooks)
#        pointing to this DAG's webhook endpoint.
#      - Select worklog events: Created, Updated, Deleted.
#      - Set the HMAC secret to the same value stored in the Variable above.
#
# 3. Rollback (BOTH steps are required — reverting only one leaves the
#    integration dead or processing into /dev/null):
#      a. DAG revert: re-deploy the previous version of master_dag.py /
#         replicon_child_dag.py; clear any in-progress DAG runs.
#      b. JIRA revert: disable or reconfigure the system webhook so JIRA
#         stops calling the new endpoint (otherwise events accumulate with
#         no consumer and may replay once the DAG is re-enabled).
# ---------------------------------------------------------------------------
