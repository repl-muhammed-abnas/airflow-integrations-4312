# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.psa_resource_assignment.config import *

environment = 'production'

instance = "production"
company_key = 'dxctechnology'

dag_id_postfix = f'{instance}'


replicon_conn_id = 'dxctechnology-replicon-RepliconIntPSA'
sftp_conn_id = 'sftp_dxctechnology_628172_PSA'
pgp_conn_id = 'pgp_psa_resource_assignment'

input_filepath = '/Production/Inbound/Resource Assignments/Input'
archive_filepath = '/Production/Inbound/Resource Assignments/Archive'
log_filepath = '/Production/Inbound/Resource Assignments/Logs'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
