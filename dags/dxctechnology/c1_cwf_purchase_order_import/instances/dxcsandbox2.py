# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_cwf_purchase_order_import.config import *

environment = 'pre-production'

instance = "dxcsandbox2"
company_key = 'dxcsandbox2'
replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntC1'

sftp_conn_id = "dxcsandbox2-sftp-628172_C1"
input_filepath = "/Test/Inbound/CWFPOBalances/Input"
archive_filepath = "/Test/Inbound/CWFPOBalances/Archive"
log_filepath = "/Test/Inbound/CWFPOBalances/Logs"

integration_report_name = "User list for purchase and worker order - Replicon"
key_namespace = "DXC_PurchaseOrderRateTypesBalanceDetails"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
