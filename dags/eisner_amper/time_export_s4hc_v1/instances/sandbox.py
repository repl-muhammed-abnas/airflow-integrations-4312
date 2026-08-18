# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.time_export_s4hc_v1.config import *

instance = "sandbox"
company_key = 'EisnerAmperSandbox'
replicon_conn_id = 'EisnerAmperSandbox_repliconint_export'
sftp_conn_internal_id = "sftp_replicon_521759"
environment = 'pre-production'

tenant_email = 'Amit.tiwari@eisneramper.com,Richa.sinha@eisneramper.com,sap.integration.support@eisneramper.com,sap.proserv.support@eisneramper.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

invalid_data_export_path = "/Sandbox/Time Data to S4HC/Error/"
valid_data_export_path = "/Sandbox/Time Data to S4HC/valid/"
valid_data_export_backup_path = "/Sandbox/Time Data to S4HC/BackUp/"
input_data_export_path = "/Sandbox/Time Data to S4HC/Input/"

send_data_to_endpoint = True

token_var = f"{company_key}_{instance}_service_token"

http_conn_id = "eisnersandbox_endpoint_http"