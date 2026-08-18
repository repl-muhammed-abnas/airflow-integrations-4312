# pylint: disable=wildcard-import unused-wildcard-import
from crl.time_export_us_v2.config import *
from crl.time_export_us_v2.mapper.paycode_mapper import PAY_CODE_MAPPER
from crl.time_export_us_v2.mapper.employee_types_mapper import EMPLOYEE_TYPE_MAPPER, PROJECT_EMPLOYEE_TYPE_MAPPER

pay_code_mapper = PAY_CODE_MAPPER
non_project_employee_types = EMPLOYEE_TYPE_MAPPER
project_employee_types = PROJECT_EMPLOYEE_TYPE_MAPPER

instance = "trial"

company_key = "CharlesRiverLaboratoriestrial01"

replicon_conn_id = "CharlesRiverLaboratoriestrial01_replicon_admin"
sftp_conn_id = "rsftp-useast_for_testing"
http_conn_id = 'crltrial01_timedata_http_conn'
http_conn_id_dev = 'crltrial01_timedata_http_conn_dev'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

export_locations = "USA"

time_export_process_export_dag_id = f"crl_us_time_export_master_{instance}_v2"

timeexport_upload_input_filepath = "/crl/Outbound/Time Export/Input"
logs_filepath = "/crl/Outbound/Time Export/Logs"

disabled = True
