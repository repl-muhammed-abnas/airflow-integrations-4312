# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from mammoet.time_data_import_v4.config import *

instance = "trial"

region = 'eu-central-1'
environment = "pre-production"

company_key = "mammoettrial01"

replicon_conn_id = "mammoettrial01_replicon_admin"
sftp_conn_id = 'sftp_useast2'

log_filepath = '/Mammoet/TimeData/Logs'

mammoet_timedata_bearer_token_variable = "mammoet_timedata_bearer_token_variable_trial"
can_run_batch_task= f'mammoet_timedata_import_child_can_run_batch_task_{instance}'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

timedata_child_dag_id = f"mammoet_timedata_import_child_{instance}_v4"
process_each_timeentry_dagid = f"mammoet_timedata_import_process_each_timeentry_child_{instance}_v4"
process_each_user_dagid = f"mammoet_timedata_import_process_each_user_child_{instance}_v4"
process_log_generation = f"mammoet_timedata_import_process_log_generation_child_{instance}_v4"

disabled = True
