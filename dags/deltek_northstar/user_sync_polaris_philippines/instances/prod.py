# pylint: disable=wildcard-import unused-wildcard-import
from deltek_northstar.user_sync_polaris_philippines.config import *
from deltek_northstar.user_sync_polaris_philippines.mapper.pay_period_mapper import pay_period_mapper
from deltek_northstar.user_sync_polaris_philippines.mapper.timeoff_type_mapper import timeoff_type_mapper

environment = 'production'
instance = "prod"

company_key = 'Deltekps'

replicon_conn_id = 'deltekps_replicon_repliconint'
deltek_cospoint_conn_id = 'deltekps_http_polarisuser'
sftp_conn_id = "sftp_deltekps_polaris"

# https://cp.deltek.com/cprestfulws/cpwwsgenericexport.cps?system=BOOKSMSS&company=PHI
api_endpoint = 'cprestfulws/cpwwsgenericexport.cps'


tenant_email = "MichaelOConnell@deltek.com,SunilMaru@deltek.com"
internal_logs_email = "rit.internallogs@replicon.com"
alert_email = "integrationalerts@replicon.com"

master_dag = f'deltek_northstar_user_sync_master_PHL_{instance}'
process_users = f'deltek_northstar_user_sync_process_each_user_child_PHL_{instance}'
processs_supervisor = f'deltek_northstar_user_sync_process_pending_supervisor_child_PHL_{instance}'
process_new_users = f'deltek_northstar_user_sync_process_new_users_child_PHL_{instance}'
process_update_users = f'deltek_northstar_user_sync_process_update_users_child_PHL_{instance}'
process_log_generation = f'deltek_northstar_user_sync_process_log_generation_child_PHL_{instance}'
process_disable_users = f'deltek_northstar_user_sync_process_disable_users_child_PHL_{instance}'
process_groups = f'deltek_northstar_user_sync_process_groups_child_PHL_{instance}'
process_dropdowns = f'deltek_northstar_user_sync_process_dropdowns_child_PHL_{instance}'
process_new_departments = f'deltek_northstar_user_sync_process_new_departments_child_PHL_{instance}'

can_run_batch_task = f'deltek_northstar_user_sync_can_run_batch_task_PHL_{instance}'
can_use_conf_payload_var_name = f'deltek_northstar_user_sync_can_use_conf_payload_var_PHL_{instance}'
last_run_date_var_name = f'deltek_northstar_user_sync_last_run_date_PHL_{instance}'

log_filepath = '/Logs'

PAY_PERIOD_MAPPER = pay_period_mapper
TIMEOFF_TYPE_MAPPER = timeoff_type_mapper
