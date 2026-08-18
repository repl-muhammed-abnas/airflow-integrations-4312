# pylint: disable=wildcard-import unused-wildcard-import
from alvarezandmarsalholdings.time_export.time_export_s4hc.config import *
from alvarezandmarsalholdings.time_export.time_export_s4hc.mappers.sbu_character_code_mapper import sbu_character_code
from alvarezandmarsalholdings.time_export.time_export_s4hc.mappers.job_category_character_code_mapper import job_category_character_code
from alvarezandmarsalholdings.time_export.time_export_s4hc.mappers.time_off_mapper import timeoff_type_project_code

instance = "trial"

company_key = "alvarezandmarsalholdingsdevtrial01"

replicon_conn_id = "alvarezandmarsalholdingsdevtrial01_replicon_radmin.1"
sftp_conn_id = "sftp_useast2"

http_conn_id = f'alvarezandmarsalholdings_timeexport_s4hc_http_conn_{instance}'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

time_export_post_to_s4hc_child_dag_id = f"alvarezandmarsalholdings_time_export_post_data_to_s4hc_api_endpoint_child_{instance}"
time_export_to_s4hc_dag_id = f"alvarezandmarsalholdings_time_export_process_time_export_s4hc_child_{instance}"

timeexport_upload_backup_filepath = "/alvarezandmarsal/s4hc_time_export/backup"

JOB_CATEGORY_CHAR_CODE = job_category_character_code
SBU_CHAR_CODE = sbu_character_code
TIME_OFF_TYPE_PROJECT_CODE= timeoff_type_project_code
