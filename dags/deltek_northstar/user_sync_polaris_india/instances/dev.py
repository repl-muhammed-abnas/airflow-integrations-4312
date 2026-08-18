# pylint: disable=wildcard-import unused-wildcard-import
from deltek_northstar.user_sync_polaris_india.config import *
from deltek_northstar.user_sync_polaris_india.mapper.pay_period_mapper import pay_period_mapper
from deltek_northstar.user_sync_polaris_india.mapper.timeoff_type_mapper import timeoff_type_mapper
from deltek_northstar.user_sync_polaris_india.mapper.workweek_mapper import workweek_mapper

environment = 'pre-production'
instance = "dev"

company_key = 'DeltekMakatiSB'
sftp_conn_id = "sftp_useast2"
replicon_conn_id = f'deltek_costpoint_polaris_makati_admin_{instance}'
deltek_cospoint_conn_id = 'deltek_costpoint_india_hris'

#https://tcmobile-dev.deltek.com/CPWeb/cprestfulws/cpwwsgenericexport.cps?system=CP7DEV1&company=USA
api_endpoint = 'CPWeb/cprestfulws/cpwwsgenericexport.cps'


tenant_email = "divyakattathara@deltek.com,jannadeguzman@deltek.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version = "" # _v1, _v2, _v3 etc.
location = "IND" # PHL, USA etc.

dag_post_fix = f"{location}_{instance}{version}" # IND_dev

master_dag = f'deltek_costpoint_user_sync_master_{dag_post_fix}'
process_users = f'deltek_costpoint_user_sync_process_each_user_child_{dag_post_fix}'
processs_supervisor = f'deltek_costpoint_user_sync_process_pending_supervisor_child_{dag_post_fix}'
process_new_users = f'deltek_costpoint_user_sync_process_new_users_child_{dag_post_fix}'
process_update_users = f'deltek_costpoint_user_sync_process_update_users_child_{dag_post_fix}'
process_log_generation = f'deltek_costpoint_user_sync_process_log_generation_child_{dag_post_fix}'
process_disable_users = f'deltek_costpoint_user_sync_process_disable_users_child_{dag_post_fix}'
process_groups = f'deltek_costpoint_user_sync_process_groups_child_{dag_post_fix}'
process_dropdowns = f'deltek_costpoint_user_sync_process_dropdowns_child_{dag_post_fix}'
process_new_departments = f'deltek_costpoint_user_sync_process_new_departments_child_{dag_post_fix}'

can_run_batch_task = f'deltek_costpoint_user_sync_can_run_batch_task_{dag_post_fix}'
can_use_conf_payload_var_name = f'deltek_costpoint_user_sync_can_use_conf_payload_var_{dag_post_fix}'
last_run_date_var_name = f'deltek_costpoint_user_sync_last_run_date_{dag_post_fix}'

log_filepath = '/shivam/cospoint/hris/logs'

PAY_PERIOD_MAPPER = pay_period_mapper
TIMEOFF_TYPE_MAPPER = timeoff_type_mapper
WORKWEEK_MAPPER = workweek_mapper
