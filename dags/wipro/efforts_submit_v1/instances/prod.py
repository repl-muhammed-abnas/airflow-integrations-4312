# pylint: disable=wildcard-import unused-wildcard-import
from wipro.efforts_submit_v1.config import *
from wipro.efforts_submit_v1.country_mapper.country_mapper_list import country_list_trial
from wipro.efforts_submit_v1.country_mapper.query_mapper_for_country import query_mapper_for_contry_trial

instance = "production"
environment = "production"
company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_replicon_repliconint"
sftp_conn_id = "sftp_internal"

alert_mail = "replicon.log.ext@wipro.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
log_filepath = "/wipro/efforts_submit/logs"

time_export_for_country = [ k  for k,v in country_list_trial.items() ]
time_export_for_country_code = country_list_trial
query_mapper_for_contry = query_mapper_for_contry_trial

master_dag = f"wipro_efforts_submission_process_project_time_master"
process_ot_perday_child = f"wipro_efforts_submission_process_project_ot_time_child_process_perday"
process_ot_time_period_child = f"wipro_efforts_submission_process_project_ot_time_child_process_period"
process_time_period_child = f"wipro_efforts_submission_process_project_time_child_process_period"
process_perday_child = f"wipro_efforts_submission_process_project_time_child_process_perday"
submit_data_child = f"wipro_efforts_submission_submit_project_time_child"
submit_ot_data_child = f"wipro_efforts_submission_submitoef_type_time_child"

can_run_batch_task = f"wipro_efforts_submit_{instance}_can_run_batch_task"
wipro_efforts_submission_bearer_token_variable = f"wipro_efforts_submission_bearer_token_variable_{instance}"
