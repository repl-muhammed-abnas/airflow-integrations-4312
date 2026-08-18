from deltek_vantagepoint.project_sync.config import *


company_key = 'vpintegration'
replicon_conn_id = f'vp_{company_key}_replicon_conn'
deltek_vantagepoint_conn_id = f'vp_{company_key}_vp_conn'

basic_auth_user_var = f'deltek_vantagepoint_webhook_username_{company_key}'
basic_auth_pass_var = f'deltek_vantagepoint_webhook_password_{company_key}'
can_run_batch_task_var_name = f'Vantagepoint_project_sync_can_run_batch_task_{company_key}'

tenant_email = 'MPTeamReplicon@deltek.com'
internal_email = 'MPTeamReplicon@deltek.com'