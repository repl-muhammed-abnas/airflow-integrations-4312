from mercury_systems_inc.time_import.config import *

environment = 'production'

instance = "prod"
company_key = "MercurySystemsInc"

replicon_conn_id = "mercurysystemsinc_replicon_repliconint"

sftp_conn_id = "sftp_mercury_systems_inc"

sftp_input_file_path = "/Production/FactoryLogix/Input/"
sftp_archive_file_path = "/Production/FactoryLogix/Processed/"
sftp_log_file_path = "/Production/FactoryLogix/Log/"

tenant_email = "RepliconAdmin@mrcy.com"
internal_logs_email = "{{ var.value.dagrun_internal_log_email }}"
alert_email = "{{ var.value.dagrun_failure_alert_email }}"

master_dag_id = f"mercury_systems_inc_time_import_master_{instance}"
process_time_data_dag_id = f"mercury_systems_inc_process_time_data_child_{instance}"
