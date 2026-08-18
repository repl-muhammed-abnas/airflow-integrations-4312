# pylint: disable=wildcard-import unused-wildcard-import
from step.project_task_import.config import *
instance = "trial"
environment = "pre-production"
company_key = "STEPafmig"
replicon_conn_id = "Stepafmig_replicon_admin"
sftp_conn_id = "sftp_useast2"
sftp_import_file_path = "/stepafmig/step.projectimports/Input"
sftp_archive_file_path = "/stepafmig/step.projectimports/Archive/"
sftp_from_address_file_path = "/stepafmig/step.projectimports/fromaddress/"
tenant_mail = '{{ var.value.dagrun_internal_testing_email }}'
alert_mail = '{{ var.value.dagrun_failure_alert_email }}'
disabled=True
