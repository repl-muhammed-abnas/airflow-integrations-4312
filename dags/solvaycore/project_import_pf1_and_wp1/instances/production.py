# pylint: disable=wildcard-import unused-wildcard-import
from solvaycore.project_import_pf1_and_wp1.config import *

instance="production"
environment="production"
company_key="Solvaycore"
replicon_conn_id="solvaycore_replicon_admin"
sftp_conn_id="sftp_solvaycore_replicon"
sftp_log_file_upload_path="/s3-ew1-mft-p-tfy-632266432872/Replicon/OUT/Project import/Logs/"
sftp_input_file_path={"pf1":"/s3-ew1-mft-p-tfy-632266432872/Replicon/OUT/Project import/Input/PF1/" , "wp1":"/s3-ew1-mft-p-tfy-632266432872/Replicon/OUT/Project import/Input/WP1/"}
sftp_archive_file_path="/s3-ew1-mft-p-tfy-632266432872/Replicon/OUT/Project import/Archive/"
# pylint: disable=line-too-long
tenant_email = "stellar.feng@solvay.com,Krishnakumar.Adhimoolam-ext@solvay.com,Suhas.jayaswamy-ext@solvay.com,Shaithanya.sidhanatham-ext@solvay.com,Ruparajeswari.madina-ext@solvay.com,Silvia.Jeleva@solvay.com"
internal_log_emails = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
