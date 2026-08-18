# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_cwf_purchase_order_import.config import *

environment = 'production'

instance = "production"
company_key = 'DXCTechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntC1'

sftp_conn_id = "DXCTechnology-sftp-628172_C1"
input_filepath = "/Production/Inbound/CWFPOBalances/Input"
archive_filepath = "/Production/Inbound/CWFPOBalances/Archive"
log_filepath = "/Production/Inbound/CWFPOBalances/Logs"

integration_report_name = "User list for purchase and worker order - Replicon"
key_namespace = "DXC_PurchaseOrderRateTypesBalanceDetails"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
