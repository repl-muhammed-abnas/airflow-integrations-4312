from mercury_systems_inc.project_import.config import *

instance = "trial"

environment = 'pre-production'

company_key = 'MercurySystemsIncSB'

replicon_conn_id = "mercury_systems_inc_replicoint"

sftp_conn_id = "sftp_useast2"

input_filepath = "/Test/Project/input/"
archive_filepath = "/Test/Project/archive/"
log_filepath = "/Test/Project/log/"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f"mercury_systems_inc_project_import_master_{instance}"
process_project_dag_id = f"mercury_systems_inc_project_import_process_project_child_{instance}"

disabled=True
