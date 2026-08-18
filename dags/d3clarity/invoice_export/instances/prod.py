# pylint: disable=wildcard-import unused-wildcard-import
from d3clarity.invoice_export.config import *

instance = "prod"
environment = 'production'
company_key = 'd3clarity'


replicon_conn_id = f'standard_xero_{company_key}_replicon'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

can_run_batch_task_var_name = f'standard_xero_connector_{company_key}_invoice_export_{instance}_can_run_batch_task'

master_dag = f"standard_xero_connector_{company_key}_invoice_export_{instance}"
child_dag = f"standard_xero_connector_{company_key}_invoice_export_child_dag_{instance}"
