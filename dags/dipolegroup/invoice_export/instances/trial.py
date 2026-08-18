instance = 'trial'
region = 'us-east-1'

environment = 'production'

company_key = "abc6"

execution_timeout_days = 14
child_dag_max_active_runs = 10

replicon_conn_id = 'standard_xero_abc6_replicon'
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_xero'
hmac_secret = 'airflow_connector_ui_hmac_secret'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f'standard_xero_connector_invoice_export_{instance}_can_run_batch_task'

provider = 'xero'
workflow = 'invoice_export'
max_active_runs_invoice_export_child = 5
