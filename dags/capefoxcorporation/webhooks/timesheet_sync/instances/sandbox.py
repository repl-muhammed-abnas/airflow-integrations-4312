# pylint: disable=wildcard-import unused-wildcard-import
from capefoxcorporation.webhooks.timesheet_sync.config import *

instance = 'sandbox'
company_key = 'capefoxcorporationsb'
replicon_conn_id = 'capefoxcorporationsb_replicon_integration'

webhook_master_dag_id = f'capefoxcorporation_deltek_costpoint_timesheet_sync_webhook_master_{instance}'
webhook_secret_var = f'capefoxcorporation_deltek_costpoint_timesheet_sync_approved_timesheets_webhook_secret_{instance}'

timesheet_sync_master_dag_id = f'capefoxcorporation_deltek_costpoint_timesheet_sync_master_{instance}'
