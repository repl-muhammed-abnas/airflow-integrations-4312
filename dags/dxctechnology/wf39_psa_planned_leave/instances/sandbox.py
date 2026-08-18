# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.wf39_psa_planned_leave.config import *

environment = 'pre-production'

instance = "sandbox"

company_key = 'dxcsandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntPSA'
sftp_conn_id = 'sftp_dxctechnology_psa'
pgp_conn_id = 'pgp_dxcsandbox_psa_planned_leave'


sftp_upload_path = "/Test/Outbound/Planned Leave"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
