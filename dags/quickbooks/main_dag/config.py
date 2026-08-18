region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14
master_dag_max_active_runs = 1

schedule_interval = '*/15 * * * *'

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'
hmac_secret = 'airflow_connector_ui_hmac_secret'

invoice_status_update = 'invoice_status_update_import'
client = 'client_import'
invoice = 'invoice_export'
workflows = ['client_import','invoice_export','invoice_status_update_import']
connector_name = 'quickbooks'
