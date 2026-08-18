# pylint: disable=wildcard-import unused-wildcard-import
from partslifeinc.time_entry.config import *
from datetime import timedelta

instance = 'prod'
environment = 'production'
company_key = 'PartsLifeInc'
replicon_conn_id = 'PartsLifeInc_replicon_Repliconint'
sftp_conn_id = 'sftp_PartsLifeInc_689920'

tenant_email = 'andrew@partslifeinc.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/Time_Punch_Import/Prod/input'
archive_filepath = '/Time_Punch_Import/Prod/archive'

can_run_batch_task_var_name = f'partslifeinc_time_entry_can_run_batch_task_var_name_{instance}'

log_file_download_link_expiry_in_sec = 7*24*60*60

schedule_interval = timedelta(seconds=30)

master_dagid = f'partslifeinc_time_entry_master_{instance}'
process_time_entry_child_dagid = f'partslifeinc_time_entry_data_process_each_user_child_{instance}'
