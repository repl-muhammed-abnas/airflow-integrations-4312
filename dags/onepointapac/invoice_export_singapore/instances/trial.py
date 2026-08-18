from onepointapac.invoice_export_singapore.config import *
from onepointapac.invoice_export_singapore.mapper import xero_mappings

instance = 'trial'

company_key = 'OnepointAPACafmig'
replicon_conn_id = 'onepointapac_replicon_trial'
xero_conn_id = 'onepointapac_xero_trial'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
provider = 'xero'

can_run_batch_task_var_name = f'onepointapac_invoice_export_singapore_{instance}_can_run_batch_task'
last_sync_time_var_name = f'onepointapac_invoice_export_singapore_{instance}_last_sync_time'
master_dag_id = f"onepointapac_invoice_export_singapore_master_{instance}"
child_dag_id = f"onepointapac_invoice_export_singapore_child_{instance}"


ITEM_CODE_BY_BILLING_TYPE = xero_mappings.ITEM_CODE_BY_BILLING_TYPE
ALLOWED_ITEM_TYPES = xero_mappings.ALLOWED_ITEM_TYPES
LINE_AMOUNT_TYPE_BY_BILLING_TYPE = xero_mappings.LINE_AMOUNT_TYPE_BY_BILLING_TYPE
DEFAULT_LINE_AMOUNT_TYPE = xero_mappings.DEFAULT_LINE_AMOUNT_TYPE