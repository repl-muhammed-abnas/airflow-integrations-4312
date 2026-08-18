from mercury_systems_inc.project_import.config import *

instance = "prod"

environment = 'production'

company_key = 'MercurySystemsInc'

replicon_conn_id = "MercurySystemsInc_replicon_replicoint"

sftp_conn_id = "sftp_mercury_REP"

input_filepath = "/Production/OracleRepl/Input/"
archive_filepath = "/Production/OracleRepl/Processed/"
log_filepath = "/Production/OracleRepl/Log/"

tenant_email = 'RepliconAdmin@mrcy.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f"mercury_systems_inc_project_import_master_{instance}"
process_project_dag_id = f"mercury_systems_inc_project_import_process_project_child_{instance}"
