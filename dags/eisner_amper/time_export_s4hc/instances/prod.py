# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.time_export_s4hc.config import *

instance = "prod"
company_key = 'EisnerAmper'
replicon_conn_id = 'EisnerAmper_repliconint_export'
sftp_conn_internal_id = "sftp_replicon_521759"
environment = 'production'

tenant_email = 'deekshitha.j01@infosys.com,srinivas.noule@infosys.com,swati.joshi07@infosys.com,sap.alert.replicon@eisneramper.com,ashwin.ns@infosys.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

invalid_data_export_path = "/Production/Time Data to S4HC/Error/"
valid_data_export_path = "/Production/Time Data to S4HC/valid/"
valid_data_export_backup_path = "/Production/Time Data to S4HC/BackUp/"
input_data_export_path = "/Production/Time Data to S4HC/Input/"

send_data_to_endpoint = True

token_var = f"{company_key}_{instance}_service_token"

http_conn_id = "eisner_endpoint_http"