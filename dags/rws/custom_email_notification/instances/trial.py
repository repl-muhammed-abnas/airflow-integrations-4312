# pylint: disable=wildcard-import unused-wildcard-import
from rws.custom_email_notification.config import *

region = 'eu-central-1'
instance = "trial"
environment = 'pre-production'
company_key = 'rwsafmig'
replicon_conn_id = 'rwsafmig_replicon_repliconadministrator'
max_active_runs_child = 20
child_dag_id = f'rws_send_individual_custom_email_notification_for_timesheets_child_{instance}'
can_run_batch_task_master = f'rws_send_email_notification_{instance}_can_run_batch_task'
can_run_batch_task_child = f'rws_send_individual_custom_email_{instance}_can_run_batch_task'
