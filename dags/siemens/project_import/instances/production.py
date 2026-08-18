# pylint: disable=wildcard-import unused-wildcard-import
from siemens.project_import.config import *

instance = "production"
environment = "production"
replicon_conn_id = "siemens_replicon_repliconint"
sftp_conn_id = "sftp_siemens_portugal_632979"
company_key = "SiemensPortugal"


# Trial SFTP paths
input_filepath = "/ProjectSync/Input/"
reference_archive_filepath = "/ProjectSync/Processing/"
reference_filepath = "/ProjectSync/Reference/"
logs_filepath = "/ProjectSync/Logs/"
input_archive_filepath = "/ProjectSync/Archives/"

master_dagid = f"siemens_project_import_master_{instance}"
process_project_dagid = f"siemens_project_import_process_projects_{instance}"
log_dagid = f"siemens_project_import_process_log_{instance}"


tenant_email = "fernando.l.santos@siemens.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alerts_email = '{{ var.value.dagrun_failure_alert_email }}'
