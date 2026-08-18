# pylint: disable=wildcard-import unused-wildcard-import
from momentive.annual_leave_policy_update_south_korea.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'Momentive'
replicon_conn_id = 'momentive-replicon-admin'
schedule_interval = '0 0 31 12 *'

can_run_batch_task = f'momentive_annual_leave_policy_update_south_korea_can_run_batch_task_{instance}'
