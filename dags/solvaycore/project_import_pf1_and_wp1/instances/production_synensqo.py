# pylint: disable=wildcard-import unused-wildcard-import
from solvaycore.project_import_pf1_and_wp1.config import *

instance="production_syensqocore"
environment="production"
company_key="SyensqoCore"
replicon_conn_id="syensqocore_replicon_admin"
sftp_conn_id="sftp_syensqocore_replicon"
sftp_log_file_upload_path="/OUT/Project import/Logs/"
sftp_input_file_path={"pf1":"/OUT/Project import/Input/PF1/" , "wp1":"/OUT/Project import/Input/WP1/"}
sftp_archive_file_path="/OUT/Project import/Archive/"
# pylint: disable=line-too-long
tenant_email = "sofia.pintado@syensqo.com,natrajadityakumar.sammeta1-ext@syensqo.com,anitha.pothuri1-ext@syensqo.com,laxman.chiluka1-ext@syensqo.com"
internal_log_emails = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
