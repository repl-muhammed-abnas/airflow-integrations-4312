# pylint: disable=wildcard-import unused-wildcard-import
from rws.custom_email_notification_v1.config import *

region = 'eu-central-1'
instance = "productionutc_6"

environment = 'production'
company_key = 'RWS'
replicon_conn_id = 'rws_replicon_RepliconAdministrator'

timezone='America/Denver'

max_active_runs_child = 20
child_dag_id = f'rws_send_individual_custom_email_notification_for_timesheets_child_{instance}_v1'
master_dag_id = f'rws_send_email_notification_for_timesheets_waiting_for_approval_master_{instance}_v1'
child_1_dag_id = f'rws_check_time_for_approver_and_send_notification_child_{instance}_v1'
can_run_batch_task_master = f'rws_send_email_notification_{environment}_can_run_batch_task_v1'
can_run_batch_task_child = f'rws_send_individual_custom_email_{environment}_can_run_batch_task_v1'
