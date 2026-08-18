# pylint: disable=wildcard-import unused-wildcard-import
from solvaycore.project_import_pf1_and_wp1.config import *

instance="trial"
environment="pre-production"
company_key="solvaycoreafmig"
replicon_conn_id="solvaycoreafmig_admin.user"
sftp_conn_id="sftp_useast2"
sftp_log_file_upload_path="/Solvay/Logs/"
sftp_input_file_path={"pf1":"/Solvay/Input/PF1/" , "wp1":"/Solvay/Input/WP1/"}
sftp_archive_file_path="/Solvay/Archive/"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_log_emails = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disabled = True
