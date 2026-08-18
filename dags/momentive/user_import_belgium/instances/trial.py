# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from datetime import timedelta
from momentive.user_import_belgium.config import *

region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'momentiveafmig'

replicon_conn_id = 'momentiveafmig_replicon_replicon.admin'
sftp_conn_id = 'sftp_useast2'
http_conn_id = "momentive_http_workday_pre_prod_belgium"

workday_report_endpoint = "https://services1.myworkday.com/ccx/service/customreport2/momentive/ISU_Replicon/Worker_Changes_Data-_Replicon?Country%21WID=a04ea128f43a42e59b1e6a19e8f0b374&format=json"
workday_report_http_conn_id = "workday_report_dummy_connection_id_trial"

schedule_interval = timedelta(seconds=60)

input_filepath_for_trial = '/Momentive/UserSync/Belgium/Input'
log_filepath = '/Momentive/UserSync/Belgium/logs'
archive_filepath = '/Momentive/UserSync/Belgium/archive'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

# This master's own DAGs (master + per-user fan-out).
momentive_belgium_user_sync_master_dag_id = f'momentive_belgium_user_sync_master_{instance}'
momentive_belgium_user_sync_process_each_user_dag_id = f'momentive_belgium_user_sync_process_each_user_{instance}'

# Routing targets: the shared other-countries children in
# dags/momentive/common_recipes_userimport (same ids that folder's instances/trial.py
# builds - both folders are loaded together, so 'instance' must match). The master
# triggers add / update / disable / supervisor-assignment; the children own the
# downstream time-off / policy-rehire fan-out internally.
momentive_othercountries_user_sync_add_user_child_dag_id = f'momentive_othercountries_user_sync_add_user_child_{instance}'
momentive_othercountries_user_sync_update_user_child_dag_id = f'momentive_othercountries_user_sync_update_user_child_{instance}'
momentive_othercountries_user_sync_disable_user_child_dag_id = f'momentive_othercountries_user_sync_disable_user_child_{instance}'
momentive_othercountries_user_sync_supervisor_assignment_dag_id = f'momentive_othercountries_user_sync_supervisor_assignment_{instance}'

# Belgium-specific time-off policy update_rehire child (recipe 1362490). Lives in this
# folder but joins the other-countries child ecosystem: the common update_user_timeoff_assign
# child triggers this dag_id (same f-string it builds), so 'instance' must match.
can_run_batch_task_var_name = f'momentive_user_import_othercountries_can_run_batch_task_{instance}'
momentive_othercountries_user_sync_bel_policy_rehire_child_dag_id = f'momentive_othercountries_user_sync_bel_policy_rehire_child_{instance}'
