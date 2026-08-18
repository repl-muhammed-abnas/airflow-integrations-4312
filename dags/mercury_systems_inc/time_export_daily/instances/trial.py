from mercury_systems_inc.time_export_daily.config import *

instance = "trial"

environment = 'pre-production'

company_key = 'MercurySystemsIncSB'

replicon_conn_id = "mercury_systems_inc_replicoint"

sftp_conn_id = "sftp_useast2"

sftp_export_file_path = "/MercurySytemsInc/TimeExportDaily/Test/input/"

tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_internal_testing_email }}"

master_dag_id = f"mercury_systems_inc_time_export_daily_master_{instance}"

