# pylint: disable=wildcard-import unused-wildcard-import
from arcticwolf.timeoff_import.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'arcticwolfnetworksinctrial02'
replicon_conn_id = 'arcticwolfnetworksinctrial02_replicon_admin'
workday_http_conn_id ='arcticwolf_timeoff_import_workday_http_connection'


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

log_file_download_link_expiry_in_sec = 7*24*60*60

log_filepath='/arcticwolf/timeoff_import/logs'
can_run_batch_task_var_name = f'arctic_wolf_timeoff_import_can_run_batch_task_var_name_{instance}'
last_synced_endtime = f'arctic_wolf_timeoff_import_last_synced_endtime_{instance}'

allowed_timeoffactions = ['Request Time Off', 'Correct Time Off']

master_dagid = f'arcticwolf_timeoff_import_master_{instance}'
process_timeoff_records_dagid =  f'arcticwolf_timeoff_import_process_child_{instance}'

disabled=True
