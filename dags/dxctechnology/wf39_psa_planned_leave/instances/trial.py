# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.wf39_psa_planned_leave.config import *

instance = "trial"

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-replicon-RepliconIntPSA'
sftp_conn_id = 'sftp_useast2'
pgp_conn_id = 'pgp_dxctrial01_psa_planned_leave'


sftp_upload_path = "/Test/Outbound/WF39PlannedLeave"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disable=True

disabled=True
