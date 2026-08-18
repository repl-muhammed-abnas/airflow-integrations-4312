region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14
master_dag_max_active_runs = 1

schedule_interval = '*/5 * * * *'

airflow_connector_ui_connid = 'airflow_connector_ui_endpoint'
hmac_secret = 'airflow_connector_ui_hmac_secret'


billed_status_update = 'invoice_status_update_billed'
paid_status_update = 'invoice_status_update_paid'
import_client = 'client_import'
export_client = 'client_export'
invoice = 'invoice_export'
workflows = ['client_import','client_export','invoice_export','invoice_status_update_billed','invoice_status_update_paid']

connector_name = 'xero'
