# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_cwf_purchase_order_import.config import *

instance = "dxctrial"
region = 'us-east-2'
environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'

sftp_conn_id = "sftp_useast2"
input_filepath = "/Test/Inbound/CWFPOBalances/Input"
archive_filepath = "/Test/Inbound/CWFPOBalances/Archive"
log_filepath = "/Test/Inbound/CWFPOBalances/Logs"

archive_reference_filepath = "/Test/Inbound/CWFPOBalances/reference/old"
reference_filepath = "/Test/Inbound/CWFPOBalances/reference/"

integration_report_name = "User list for purchase and worker order - Replicon"
key_namespace = "DXC_PurchaseOrderRateTypesBalanceDetails"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disable=True

disabled=True
