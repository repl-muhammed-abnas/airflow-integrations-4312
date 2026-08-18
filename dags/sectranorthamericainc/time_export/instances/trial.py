# pylint: disable=wildcard-import unused-wildcard-import
from sectranorthamericainc.time_export.config import *

instance = "trial"

company_key = "sectranorthamericainctrial01"

replicon_conn_id = "sectranorthamericainctrial01_replicon_admin"
http_conn_id = 'sectranorthamericainctrial01_timedata_http_conn'


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

time_export_master_dag_id = f"sectranorthamericainc_time_export_master_{instance}"
time_export_process_export_dag_id = f"sectranorthamericainc_time_export_process_users_per_timesheet_period_{instance}"


AZURE_API_ENDPOINT = "sectraintegrations.servicebus.windows.net/replicontest/messages"
client_secrete_var_name = f"sectranorthamericainc_azure_client_id_secret_{instance}_tokens"

SHOULD_USE_REPORT = True

disabled=True
