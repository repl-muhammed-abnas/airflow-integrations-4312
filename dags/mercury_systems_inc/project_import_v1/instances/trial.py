from mercury_systems_inc.project_import_v1.config import *

instance = "trial"

environment = 'pre-production'

company_key = 'MercurySystemsIncSB'

replicon_conn_id = "mercurysystemsincsb_replicon_replicoint"

sftp_conn_id = "sftp_internal_useast2"

input_filepath = "/Test/Project/input/"
archive_filepath = "/Test/Project/archive/"
log_filepath = "/Test/Project/log/"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dag_id = f"mercury_systems_inc_project_import_master_{instance}_v1"
process_project_dag_id = f"mercury_systems_inc_project_import_process_project_child_{instance}_v1"
