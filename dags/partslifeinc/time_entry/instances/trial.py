# pylint: disable=wildcard-import unused-wildcard-import
from partslifeinc.time_entry.config import *
from datetime import timedelta


instance = 'trial'
environment = 'pre-production'
company_key = 'partslifeinctrial01'
replicon_conn_id = 'partslifeinctrial01_replicon_admin'
sftp_conn_id = 'sftp_partslifeinctrial01_689920'

tenant_email = 'andrew@partslifeinc.com'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/Time_Punch_Import/trial/input'
archive_filepath = '/Time_Punch_Import/trial/archive'

can_run_batch_task_var_name = f'partslifeinc_time_entry_can_run_batch_task_var_name_{instance}'

log_file_download_link_expiry_in_sec = 7*24*60*60

schedule_interval = timedelta(seconds=60)

master_dagid = f'partslifeinc_time_entry_master_{instance}'
process_time_entry_child_dagid = f'partslifeinc_time_entry_data_process_each_user_child_{instance}'

disabled=True
