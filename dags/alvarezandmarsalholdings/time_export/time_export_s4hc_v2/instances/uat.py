# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.time_export.time_export_s4hc_v2.config import *
from alvarezandmarsalholdings.time_export.time_export_s4hc_v2.mappers.sbu_character_code_mapper import sbu_character_code
from alvarezandmarsalholdings.time_export.time_export_s4hc_v2.mappers.job_category_character_code_mapper import job_category_character_code
from alvarezandmarsalholdings.time_export.time_export_s4hc_v2.mappers.time_off_mapper import timeoff_type_project_code

instance = "UAT"

company_key = "AlvarezandMarsalHoldingsUAT"

replicon_conn_id = "alvarezandmarsalholdingsuat_replicon_radmin1"
sftp_conn_id = "sftp_alvarezandmarsalholdingsuat_621229"

http_conn_id = f'alvarezandmarsalholdings_timeexport_s4hc_http_conn_{instance}'

tenant_email = 'ITERP@alvarezandmarsal.com,6d20463c.itinfoalvarezandmarsal.onmicrosoft.com@amer.teams.ms'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

version = "v2"

time_export_post_to_s4hc_child_dag_id = f"alvarezandmarsalholdings_time_export_post_data_to_s4hc_api_endpoint_child_{instance}_{version}"
time_export_to_s4hc_dag_id = f"alvarezandmarsalholdings_time_export_process_time_export_s4hc_child_{instance}_{version}"

can_post_to_api_endpoint = f"AlvarezandMarsalHoldings_time_export_s4hc_can_post_to_api_endpoint_{instance}"

timeexport_upload_backup_filepath = "/UAT/Time Extract to S4/Output File"

JOB_CATEGORY_CHAR_CODE = job_category_character_code
SBU_CHAR_CODE = sbu_character_code
TIME_OFF_TYPE_PROJECT_CODE= timeoff_type_project_code
