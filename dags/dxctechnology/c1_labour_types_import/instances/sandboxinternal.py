# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_labour_types_import.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = 'dxcsandboxinternal'
company_key = 'dxcsandboxinternal'
replicon_conn_id = 'dxcsandboxinternal-replicon-RepliconIntC1'
sftp_conn_id = "sftp_Airflowmig_useast2"
input_filepath = "/Test/Inbound/C1TaskandLabortypes/LaborTypeInput"
archive_filepath = "/Test/Inbound/C1TaskandLabortypes/LaborTypeInputLogs"
log_filepath = "/Test/Inbound/C1TaskandLabortypes/Archive"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disable=True

disabled=True
