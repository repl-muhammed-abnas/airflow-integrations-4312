# pylint: disable=wildcard-import unused-wildcard-import
from sectranorthamericainc.time_export.config import *

instance = "prod"
environment = "production"

company_key = "sectranorthamericainc"

replicon_conn_id = "sectranorthamericainc_replicon_admin"
http_conn_id = f'sectranorthamericainc_timedata_http_conn_{instance}'


tenant_email = 'support-replicon@sectra.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_log_email }}'

time_export_master_dag_id = f"sectranorthamericainc_time_export_master_{instance}"
time_export_process_export_dag_id = f"sectranorthamericainc_time_export_process_users_per_timesheet_period_{instance}"

AZURE_API_ENDPOINT = "sectraintegrations.servicebus.windows.net/repliconprod/messages"
client_secrete_var_name = f"sectranorthamericainc_azure_client_id_secret_{instance}_tokens"

SHOULD_USE_REPORT = True
