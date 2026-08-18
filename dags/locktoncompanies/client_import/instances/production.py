#pylint: disable=wildcard-import unused-wildcard-import
from locktoncompanies.client_import.config import *
region = 'us-east-1'
instance = "production"
environment = 'production'
company_key = 'LocktonCompanies'
replicon_conn_id = 'locktoncompanies_replicon_admin'
sftp_conn_id = 'sftp_locktoncompanies_548722'
sftp_internal_conn_id = 'sftp_useast2'

tenant_email = "HWilliams@lockton.com, jclarke@lockton.com,testlund@lockton.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

max_active_runs_child = 5
max_active_parallel_runs = 5

input_filepath = '/Production/Archives/Reference/'
reference_filepath = '/LocktonCompanies/Reference/'
archive_filepath = '/LocktonCompanies/Archive/'

can_run_batch_task = f'locktoncompanies_client_import_can_run_batch_task_{instance}'
