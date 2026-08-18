from eisner_amper.timeoff_import_workday.config import *

instance = "trial"

environment = 'pre-production'

company_key = 'eisnerampertrial01'

replicon_conn_id = "eisnerampertrial01_replicon_radmin"
sftp_conn_id = "sftp_useast2"

input_filepath = '/EisnerAmper/Sandbox/Time Off Data to Replicon/Input'
archive_filepath = '/EisnerAmper/Sandbox/Time Off Data to Replicon/Archive'
log_filepath = '/EisnerAmper/Sandbox/Time Off Data to Replicon/Logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

version = "" # _v1, _v2 etc.

dag_id_prefix = f"{instance}{version}"

master_dagid = f"eisner_amper_timeoff_import_workday_master_{dag_id_prefix}"
process_unique_users_child = f"eisner_amper_timeoff_import_workday_process_users_child_{dag_id_prefix}"
process_each_entry_child = f"eisner_amper_timeoff_import_workday_process_each_entry_child_{dag_id_prefix}"
process_log_generation = f"eisner_amper_timeoff_import_workday_log_generation_{dag_id_prefix}"

can_run_batch_task = f"eisner_amper_timeoff_import_workday_can_run_batch_task_{dag_id_prefix}_var"

disabled = True
