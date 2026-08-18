# pylint: disable=wildcard-import unused-wildcard-import
from sigroup.payroll_export_china.config import *

region = 'eu-central-1'
instance = 'trial'

environment = 'pre-production'
company_key = 'sigroupafmig'

replicon_conn_id = 'sigroupafmig_replicon_admin'
sftp_conn_id = 'sftp_useast2'
secondary_sftp_conn_id = 'sftp_useast2'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

pgp_conn_id = 'pgp_sigroup_payroll_export'
file_upload_path = 'sigroup/payload/'
log_upload_path = 'sigroup/payload/'

can_run_batch_task = f'sigroup_payroll_export_china_can_run_batch_task_{instance}'

disabled=True
