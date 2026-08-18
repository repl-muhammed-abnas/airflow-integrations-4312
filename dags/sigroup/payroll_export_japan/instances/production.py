# pylint: disable=wildcard-import unused-wildcard-import
from sigroup.payroll_export_japan.config import *

region = 'eu-central-1'
instance = 'production'

environment = 'production'
company_key = 'sigroup'

replicon_conn_id = 'sigroup_replicon_admin'
secondary_sftp_conn_id = 'sftp_sigroup_664942'
sftp_conn_id = 'sftp_sigroup_sig'

tenant_email = 'Japan.HRIS@siigroup.com'
internal_logs_email = "{{ var.value.dagrun_internal_log_email }}"

pgp_conn_id = 'pgp_sigroup_payroll_export'
log_upload_path = '/Payroll Export Archive_Prod/'
file_upload_path = '/Connect/SIG001/prod/'

can_run_batch_task = f'sigroup_payroll_export_japan_can_run_batch_task_{instance}'