# pylint: disable=wildcard-import unused-wildcard-import
from siemens.project_import.config import *

instance = "trial"
replicon_conn_id = "siemens_trial_replicon"
sftp_conn_id = "sftp_useast2"
company_key = "Siemensportugalafmig"

# Trial environment specific settings
tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
# Trial SFTP paths
input_filepath = "/Trial/ProjectSync/Input"
reference_archive_filepath = "/Trial/ProjectSync/Archives/"
reference_filepath = "/Trial/ProjectSync/Reference/"
logs_filepath = "/Trial/ProjectSync/Logs/"
input_archive_filepath = "/Trial/ProjectSync/Processing/"

master_dagid = f"siemens_project_import_master_{instance}"
process_project_dagid = f"siemens_project_import_process_projects_{instance}"
log_dagid = f"siemens_project_import_process_log_{instance}"

tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alerts_email = "{{ var.value.dagrun_internal_testing_email }}"

disabled=True
