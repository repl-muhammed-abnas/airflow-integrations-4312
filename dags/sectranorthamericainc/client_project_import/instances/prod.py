# pylint: disable=wildcard-import unused-wildcard-import
from sectranorthamericainc.client_project_import.config import *

region = 'us-east-1'
instance = "prod"
environment = 'production'
company_key = 'SectraNorthAmericaInc'

replicon_conn_id = 'SectraNorthAmericaInc_replicon_trigo'

can_run_batch_task_var_name = f'sectranorthamerica_project_client_sync_{instance}_can_run_batch_task'

sectranorthamerica_webhook_bearer_token_var = f'sectranorthamerica_client_project_import_bearer_token_{instance}'

master_dag_id = f'sectranorthamerica_client_project_import_master_{instance}'
child_dag_id = f'sectranorthamerica_client_project_import_child_{instance}'

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
tenant_email = "support-replicon@sectra.com"
