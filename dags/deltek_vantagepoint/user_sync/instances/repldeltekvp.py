# pylint: disable=line-too-long
# pylint: disable=unused-import
from deltek_vantagepoint.initial_setup.instances.repldeltekvp import oefs, groups, usersync_filter_var
from deltek_vantagepoint.user_sync.config import *
region = 'us-east-1'
environment = 'pre-production'
instance = "repldeltekvp"
company_key = 'repldeltekvp'
replicon_conn_id = f'vp_{company_key}_replicon_conn'
deltek_vantagepoint_conn_id = f'vp_{company_key}_vp_conn'

can_run_batch_task_var_name = f'Vantagepoint_user_import_can_run_batch_task_{instance}'
child_dag_max_active_runs = 3

webhook_basicauth_username = f'deltek_vantagepoint_webhook_username_{company_key}'
webhook_basicauth_password = f'deltek_vantagepoint_webhook_password_{company_key}'

oefs = list(filter(lambda oef: 'user' in oef['bind'], oefs))
