# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.wf39_psa_resource_assignment_v3.config import *

instance = 'production'

environment = 'production'

company_key = 'dxctechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntPSA'
sftp_conn_id = "sftp_dxctechnology_628172_PSA"

pgp_conn_id = 'pgp_psa_resource_assignment'

input_filepath = "/Production/Inbound/C1CP Resource Assignments/c1_processing"
archive_filepath = "/Production/Inbound/C1CP Resource Assignments/Archives"
log_filepath = "/Production/Inbound/C1CP Resource Assignments/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_decrypt_file_var_name = f'dxc_wf39_psa_c1_resource_assignment_can_decrypt_file_{instance}'
