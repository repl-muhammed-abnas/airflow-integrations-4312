# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.psa_resource_assignment.config import *

environment = 'pre-production'

instance = "sandbox"
dag_id_postfix = f'{instance}'

company_key = 'dxcsandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntPSA'
sftp_conn_id = 'sftp_dxctechnology_psa'
pgp_conn_id = 'pgp_dxcsandbox_psa_resource_assignment'

input_filepath = '/Test/Inbound/Resource Assignments/Input'
archive_filepath = '/Test/Inbound/Resource Assignments/Archives'
log_filepath = '/Test/Inbound/Resource Assignments/Logs'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disable=True

disabled=True
