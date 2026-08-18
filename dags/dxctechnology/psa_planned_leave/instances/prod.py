# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.psa_planned_leave.config import *

environment = 'production'

instance = "production"

company_key = 'dxctechnology'

replicon_conn_id = 'dxctechnology-replicon-RepliconIntPSA'
sftp_conn_id = 'sftp_dxctechnology_628172_PSA'
pgp_conn_id = 'pgp_dxctechnology_psa_planned_leave'


sftp_upload_path = "/Production/Outbound/Planned Leave"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
