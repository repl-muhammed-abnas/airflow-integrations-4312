from mmr_consulting.invoice_export.config import *

instance = 'trial'
company_key = 'ConsultingServices001afmig'
replicon_conn_id = 'mmrconsulting_replicon_trial'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f'mmr_consulting_invoice_export_{instance}_can_run_batch_task'
provider = 'xero'

last_sync_time_var_name = f'mmr_consulting_invoice_export_{instance}_last_sync_time'

us = f"mmrconsulting_xero_us_{instance}"
canada = f"mmrconsulting_xero_canada_{instance}"
india = f"mmrconsulting_xero_india_{instance}"
australia = f"mmrconsulting_xero_australia_{instance}"
singapore = f"mmrconsulting_xero_singapore_{instance}"