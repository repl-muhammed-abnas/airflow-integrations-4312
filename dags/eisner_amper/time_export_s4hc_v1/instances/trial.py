# pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.time_export_s4hc_v1.config import *

instance = "trial"
company_key = 'eisnerampertrial02'
replicon_conn_id = 'eisnerampertrial02_replicon_radmin'
sftp_conn_internal_id = "sftp_useast2"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

invalid_data_export_path = "/Production/Time Data to S4HC/Error/"
valid_data_export_path = "/Production/Time Data to S4HC/valid/"
valid_data_export_backup_path = "/Production/Time Data to S4HC/BackUp/"
input_data_export_path = "/Production/Time Data to S4HC/Input/"

send_data_to_endpoint = False

token_var = f"{company_key}_{instance}_service_token"

http_conn_id = "eisner_endpoint_http"

disabled=True
