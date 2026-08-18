#pylint: disable=wildcard-import unused-wildcard-import
from locktoncompanies.client_import.config import *
region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'locktoncompaniesafmig'
replicon_conn_id = 'locktoncompaniesafmig_replicon_admin'
sftp_conn_id = 'sftp_useast2'
sftp_internal_conn_id = 'sftp_useast1'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

max_active_runs_child = 5

input_filepath = '/Rishabh/LocktonCompanies/Input/'
reference_filepath = 'Rishabh/LocktonCompanies/Reference/'
archive_filepath = '/Rishabh/LocktonCompanies/Archive/'

can_run_batch_task = f'locktoncompanies_client_import_can_run_batch_task_{instance}'

disable=True

disabled=True
