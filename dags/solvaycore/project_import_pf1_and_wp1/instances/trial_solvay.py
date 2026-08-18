# pylint: disable=wildcard-import unused-wildcard-import
from solvaycore.project_import_pf1_and_wp1.config import *

instance="trial01"
environment="pre-production"
company_key="solvaytrial01"
replicon_conn_id="solvaytrial01_replicon_akhurana"
sftp_conn_id="solvaytrial01_sftp_repicon"
sftp_log_file_upload_path="/OUT/Project import/Logs/"
sftp_input_file_path={"pf1":"/OUT/Project import/Input/PF1/" , "wp1":"/OUT/Project import/Input/WP1/"}
sftp_archive_file_path="/OUT/Project import/Archive/"
tenant_email = "laxman.chiluka-ext@solvay.com,durgaprasad.kona-ext@solvay.com,anitha.pothuri-ext@solvay.com,natrajadityakumar.sammeta-ext@solvay.com,sofia.pintado@solvay.com,Silvia.Jeleva@solvay.com"
internal_log_emails = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
