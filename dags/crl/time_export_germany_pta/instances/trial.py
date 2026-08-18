# pylint: disable=wildcard-import unused-wildcard-import
from crl.time_export_germany_pta.config import *
from crl.time_export_germany_pta.mapper.paycode_mapper import GERMANY_PAY_CODE_MAPPER

pay_code_mapper = GERMANY_PAY_CODE_MAPPER

instance = "trial"

company_key = "CharlesRiverLaboratoriestrial01"

replicon_conn_id = "charlesriverlaboratoriestrial01_replicon_repliconadmin"
sftp_conn_id = "sftp_useast2"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

export_locations = "DEU"

time_export_master_dag_id = f"crl_time_export_germany_pta_master_{instance}"
time_export_process_export_dag_id = f"crl_time_export_germany_pta_process_child_{instance}"

timeexport_upload_input_filepath = "/crl/Outbound/Time Export/Input"
logs_filepath = "/crl/Outbound/Time Export/Logs"
