from mercury_systems_inc.time_import.config import *

instance = "trial"

environment = 'pre-production'

company_key = 'MercurySystemsIncSB'

replicon_conn_id = "mercury_systems_inc_replicoint"

sftp_conn_id = "sftp_useast2"

sftp_input_file_path = "/Test/input/"
sftp_archive_file_path = "/Test/archive/"
sftp_log_file_path = "/Test/log/"

tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_internal_testing_email }}"

master_dag_id = f"mercury_systems_inc_time_import_master_{instance}"
process_time_data_dag_id = f"mercury_systems_inc_process_time_data_child_{instance}"
