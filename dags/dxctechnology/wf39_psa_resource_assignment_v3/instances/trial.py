# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.wf39_psa_resource_assignment_v3.config import *

instance = 'trial'

environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
sftp_conn_id = "rsftp_afmig_useast2"

input_filepath = "/DXC/wf39_psa/c1_processing"
archive_filepath = "/DXC/wf39_psa/Archive"
log_filepath = "/DXC/wf39_psa/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

pgp_conn_id = 'pgp_dxcsandbox_psa_resource_assignment'

can_decrypt_file_var_name = f'dxc_wf39_psa_c1_resource_assignment_can_decrypt_file_{instance}'
