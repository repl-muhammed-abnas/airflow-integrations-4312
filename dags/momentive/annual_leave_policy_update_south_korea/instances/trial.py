# pylint: disable=wildcard-import unused-wildcard-import
from momentive.annual_leave_policy_update_south_korea.config import *
region = 'us-east-1'
instance = 'trial'
environment = 'pre-production'
company_key = 'momentiveafmig'
replicon_conn_id = 'momentiveafmig_replicon_replicon.admin'
schedule_interval = '0 0 31 12 *'

can_run_batch_task = f'momentive_annual_leave_policy_update_south_korea_can_run_batch_task_{instance}'
disabled = True
