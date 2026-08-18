from mercury_systems_inc.time_export_weekly.config import *

environment = 'production'

instance = "prod"
company_key = "MercurySystemsInc"

replicon_conn_id = "mercurysystemsinc_replicon_repliconint"

sftp_conn_id = "sftp_mercury_systems_inc"

sftp_export_file_path = "/Production//ReplOracle/Input/"
tenant_email = "RepliconAdmin@mrcy.com"
internal_logs_email = "{{ var.value.dagrun_internal_log_email }}"
alert_email = "{{ var.value.dagrun_failure_alert_email }}"

master_dag_id = f"mercury_systems_inc_time_export_weekly_master_{instance}"
