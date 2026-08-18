from mercury_systems_inc.time_import.config import *

instance = "uat"

environment = 'pre-production'

company_key = 'MercurySystemsIncSB'

replicon_conn_id = "mercurysystemsincsb_replicon_replicoint"

sftp_conn_id = "sftp_mercury_systems_inc"

sftp_input_file_path = "/Test/FactoryLogix/Input/"
sftp_archive_file_path = "/Test/FactoryLogix/Processed/"
sftp_log_file_path = "/Test/FactoryLogix/Log/"

tenant_email = "RepliconAdmin@mrcy.com"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_failure_alert_email }}"

master_dag_id = f"mercury_systems_inc_time_import_master_{instance}"
process_time_data_dag_id = f"mercury_systems_inc_process_time_data_child_{instance}"
