# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from datetime import timedelta
from momentive.user_import_india.config import *

region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'momentiveafmig'

replicon_conn_id = 'momentiveafmig_replicon_replicon.admin'
sftp_conn_id = 'sftp_internal_useast2'
http_conn_id = "momentive_http_workday_pre_prod"

workday_report_endpoint = "https://services1.myworkday.com/ccx/service/customreport2/momentive/ISU_Replicon/Worker_Changes_Data-_Replicon?Country%21WID=c4f78be1a8f14da0ab49ce1162348a5e&format=json"
workday_report_http_conn_id = "workday_report_dummy_connection_id_trial"

schedule_interval = timedelta(seconds=60)

input_filepath_for_trial = '/Momentive/UserSync/India/Input'
log_filepath = '/Momentive/UserSync/India/logs'
archive_filepath = '/Momentive/UserSync/India/archive'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f'momentive_user_import_india_can_run_batch_task_{instance}'

momentive_india_user_sync_master_dag_id = f'momentive_india_user_sync_master_{instance}'
momentive_india_user_sync_child_update_user_dag_id = f'momentive_india_user_sync_update_user_child_{instance}'
momentive_india_user_sync_child_add_user_dag_id = f'momentive_india_user_sync_add_user_child_{instance}'
momentive_india_user_sync_child_disable_user_dag_id = f'momentive_india_user_sync_disable_user_child_{instance}'
momentive_india_user_sync_supervisor_assignment_dag_id = f'momentive_india_user_sync_supervisor_assignment_{instance}'
momentive_india_user_sync_child_add_timeoff_new_user_dag_id = f'momentive_india_user_sync_timeoff_new_user_child_{instance}'
momentive_india_user_sync_child_update_user_timeoff_assign_id = f'momentive_india_user_sync_update_user_timeoff_assign_child_{instance}'
momentive_india_user_sync_child_put_remaining_balance_for_payout_dag_id = f'momentive_india_user_sync_put_remaining_balance_for_payout_child_{instance}'
