from eisner_amper.timeoff_import_workday.config import *

instance = "production"

environment = 'production'

company_key = 'EisnerAmper'

replicon_conn_id = "eisneramper_repliconint.timeoffimport"
sftp_conn_id = "eisner_amper_sftp_521759"

input_filepath = '/Production/Time Off Import/Input File'
archive_filepath = '/Production/Time Off Import/Archive'
log_filepath = '/Production/Time Off Import/Error Log'

tenant_email = "hrit@eisneramper.com,sap.integration.support@eisneramper.com,sap.proserv.support@eisneramper.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version = "" # _v1, _v2 etc.

dag_id_prefix = f"{instance}{version}"

master_dagid = f"eisner_amper_timeoff_import_workday_master_{dag_id_prefix}"
process_unique_users_child = f"eisner_amper_timeoff_import_workday_process_users_child_{dag_id_prefix}"
process_each_entry_child = f"eisner_amper_timeoff_import_workday_process_each_entry_child_{dag_id_prefix}"
process_log_generation = f"eisner_amper_timeoff_import_workday_log_generation_{dag_id_prefix}"

can_run_batch_task = f"eisner_amper_timeoff_import_workday_can_run_batch_task_{dag_id_prefix}_var"
