# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_cwf_purchase_order_import.config import *

environment = 'pre-production'

instance = "dxcsandboxinternal"
company_key = 'dxcsandboxinternal'
replicon_conn_id = 'dxcsandboxinternal-RepliconIntC1'

sftp_conn_id = "sftp_useast2"
input_filepath = "/DXCSandboxInternal/Test/Inbound/CWFPOBalances/Input"
archive_filepath = "/DXCSandboxInternal/Test/Inbound/CWFPOBalances/Archive"
log_filepath = "/DXCSandboxInternal/Test/Inbound/CWFPOBalances/Logs"

integration_report_name = "User list for purchase and worker order - Replicon"
key_namespace = "DXC_PurchaseOrderRateTypesBalanceDetails"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disable=True

disabled=True
