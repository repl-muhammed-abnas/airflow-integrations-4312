region = 'us-east-1'
environment = 'pre-production'

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_xero'
hmac_secret = 'airflow_connector_ui_hmac_secret'

execution_timeout_days = 14
max_active_runs_master = 1
max_active_runs_invoice_export_child = 5

provider = 'xero'
workflow = 'invoice_export'
