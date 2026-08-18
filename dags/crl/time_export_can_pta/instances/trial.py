# pylint: disable=wildcard-import unused-wildcard-import
from crl.time_export_can_pta.config import *

instance = "trial"

company_key = "CharlesRiverLaboratoriestrial01"

replicon_conn_id = "CharlesRiverLaboratoriestrial01_replicon_admin"
sftp_conn_id = "sftp_CharlesRiverLaboratoriestrial01_adp"
http_conn_id = 'crltrial01_timedata_http_conn'
http_conn_id_dev = 'crltrial01_timedata_http_conn_dev'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

export_locations = "CAN"

time_export_master_dag_id = f"crl_pta_can_time_export_master_{instance}"
time_export_process_export_dag_id = f"crl_pta_can_time_export_process_time_export_child_{instance}"

timeexport_upload_input_filepath = "/crl/Outbound/Time Export/Input"
logs_filepath = "/crl/Outbound/Time Export/Logs"

client_id_secret_variable_name = f"crl_client_id_secret_variable_{instance}"
crl_time_export_bearer_token_variable = "crl_time_export_bearer_token_variable_qa"
disabled=True
