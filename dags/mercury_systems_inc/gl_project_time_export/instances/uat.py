from mercury_systems_inc.gl_project_time_export.config import *

instance = "uat"

environment = 'pre-production'

company_key = 'MercurySystemsIncSB'

replicon_conn_id = "mercurysystemsincsb_replicon_replicoint"

sftp_conn_id = "sftp_mercury_systems_inc"

sftp_export_file_path = "/Test/ReplGL/Input/"

tenant_email = "RepliconAdmin@mrcy.com"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_failure_alert_email }}"

master_dag_id = f'mercury_systems_inc_gl_project_time_export_{instance}'

