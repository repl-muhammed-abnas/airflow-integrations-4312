# pylint: disable=wildcard-import unused-wildcard-import
from mammoet.time_data_import_v1.config import *

instance = "prod"

region = 'eu-central-1'
environment = "production"

company_key = "mammoet"

replicon_conn_id = "mammoet_replicon_admin"
# To be updated
sftp_conn_id = 'sftp_mammoet_550793'
log_filepath = '/Production/Time Data Entry Import/Log'

mammoet_timedata_bearer_token_variable = f"mammoet_timedata_bearer_token_variable_{instance}"
can_run_batch_task= f'mammoet_timedata_import_child_can_run_batch_task_{instance}'

tenant_email = 'RepliconNotifications@mammoet.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

timedata_master_dag_id = f"mammoet_timedata_import_master_{instance}_v1"
timedata_child_dag_id = f"mammoet_timedata_import_child_{instance}_v1"
process_each_timeentry_dagid = f"mammoet_timedata_import_process_each_timeentry_child_{instance}_v1"
process_each_user_dagid = f"mammoet_timedata_import_process_each_user_child_{instance}_v1"
process_log_generation = f"mammoet_timedata_import_process_log_generation_child_{instance}_v1"
