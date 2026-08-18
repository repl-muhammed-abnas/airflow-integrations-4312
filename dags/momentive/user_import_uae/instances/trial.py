# pylint: disable=wildcard-import unused-wildcard-import
from datetime import timedelta
from momentive.user_import_uae.config import *

region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'momentiveafmig'

replicon_conn_id = 'momentiveafmig_replicon_replicon.admin'
sftp_conn_id = 'sftp_useast2'

country = 'UAE'

workday_report_http_conn_id = "workday_report_dummy_connection_id_trial"

schedule_interval = timedelta(seconds=60)

input_filepath_for_trial = '/Momentive/UserSync/UAE/input'
log_filepath = '/Momentive/UserSync/UAE/logs'
archive_filepath = '/Momentive/UserSync/UAE/archive'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f'momentive_user_import_uae_can_run_batch_task_{instance}'

# This master's own DAG + the per-user DAG it fans out to.
momentive_uae_user_sync_master_dag_id = f'momentive_uae_user_sync_master_{instance}'
momentive_uae_user_sync_process_each_user_dag_id = f'momentive_uae_user_sync_process_each_user_{instance}'

# Routing targets: the shared other-countries children in
# dags/momentive/common_recipes_userimport (same ids as that folder's instances/trial.py
# builds — both folders are loaded together; instance must match).
momentive_othercountries_user_sync_add_user_child_dag_id = f'momentive_othercountries_user_sync_add_user_child_{instance}'
momentive_othercountries_user_sync_update_user_child_dag_id = f'momentive_othercountries_user_sync_update_user_child_{instance}'
momentive_othercountries_user_sync_disable_user_child_dag_id = f'momentive_othercountries_user_sync_disable_user_child_{instance}'
momentive_othercountries_user_sync_supervisor_assignment_dag_id = f'momentive_othercountries_user_sync_supervisor_assignment_{instance}'
