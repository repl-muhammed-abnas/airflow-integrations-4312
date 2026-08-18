from mercury_systems_inc.project_import.config import *

instance = "uat"

environment = 'pre-production'

company_key = 'MercurySystemsIncSB'

replicon_conn_id = "mercurysystemsincsb_replicon_replicoint"

sftp_conn_id = "sftp_mercury_systems_inc"

input_filepath = "/Test/OracleRepl/Input/"
archive_filepath = "/Test/OracleRepl/Processed/"
log_filepath = "/Test/OracleRepl/Log/"

tenant_email = 'RepliconAdmin@mrcy.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f"mercury_systems_inc_project_import_master_{instance}"
process_project_dag_id = f"mercury_systems_inc_project_import_process_project_child_{instance}"

disabled=True
