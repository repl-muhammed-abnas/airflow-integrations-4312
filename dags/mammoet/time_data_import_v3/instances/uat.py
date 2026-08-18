# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.time_data_import_v3.config import *

instance = "uat"

region = 'eu-central-1'
environment = "pre-production"

company_key = "mammoettrial01"

replicon_conn_id = "mammoettrial01_replicon_admin"
sftp_conn_id = 'sftp_mammoet_uat'

log_filepath = '/Time Data Entry Import/Trial01/Log'

mammoet_timedata_bearer_token_variable = "mammoet_timedata_bearer_token_variable_uat"
can_run_batch_task= f'mammoet_timedata_import_child_can_run_batch_task_{instance}'

tenant_email = 'repliconnotifications@mammoet.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

timedata_child_dag_id = f"mammoet_timedata_import_child_{instance}_v3"
process_each_timeentry_dagid = f"mammoet_timedata_import_process_each_timeentry_child_{instance}_v3"
process_each_user_dagid = f"mammoet_timedata_import_process_each_user_child_{instance}_v3"
process_log_generation = f"mammoet_timedata_import_process_log_generation_child_{instance}_v3"
