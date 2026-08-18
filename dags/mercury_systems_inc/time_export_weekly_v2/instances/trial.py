from mercury_systems_inc.time_export_weekly_v2.config import *

instance = "trial"

version = "_v1"

environment = 'pre-production'

company_key = 'MercurySystemsInctrial01'

replicon_conn_id = "MercurySystemsInctrial01_inc_replicoint"

sftp_conn_id = "sftp_useast2"

sftp_export_file_path = "/MercurySytemsInc/TimeExport/Test/input/"

tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_internal_testing_email }}"

master_dag_id = f"mercury_systems_inc_time_export_weekly_master_{instance}{version}"
