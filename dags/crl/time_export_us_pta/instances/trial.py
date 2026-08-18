# pylint: disable=wildcard-import unused-wildcard-import
from crl.time_export_us_pta.config import *
from crl.time_export_us_pta.mapper.paycode_mapper import PAY_CODE_MAPPER

pay_code_mapper = PAY_CODE_MAPPER

instance = "trial"

company_key = "CharlesRiverLaboratoriestrial01"

replicon_conn_id = "CharlesRiverLaboratoriestrial01_replicon_admin"
sftp_conn_id = "sftp_CharlesRiverLaboratoriestrial01_adp"
http_conn_id = 'crltrial01_timedata_http_conn'
http_conn_id_dev = 'crltrial01_timedata_http_conn_dev'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

export_locations = "USA"

time_export_master_dag_id = f"crl_pta_us_time_export_master_{instance}"
time_export_process_export_dag_id = f"crl_pta_us_time_export_process_time_export_child_{instance}"

timeexport_upload_input_filepath = "/crl/Outbound/Time Export/Input"
logs_filepath = "/crl/Outbound/Time Export/Logs"
disabled=True
