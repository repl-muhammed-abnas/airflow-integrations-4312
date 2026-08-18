# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.wf39_psa_resource_assignment_compass_v2.config import *

instance = 'sandbox2'

environment = 'pre-production'

company_key = 'dxcsandbox2'
replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntPSA'
sftp_conn_id = "sftp_dxcsandbox2_psa"

pgp_conn_id = 'pgp_dxcsandbox_psa_resource_assignment'

input_filepath = "/Test/Inbound/C1CP Resource Assignments/compass_processing"
archive_filepath = "/Test/Inbound/C1CP Resource Assignments/Archives"
log_filepath = "/Test/Inbound/C1CP Resource Assignments/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
