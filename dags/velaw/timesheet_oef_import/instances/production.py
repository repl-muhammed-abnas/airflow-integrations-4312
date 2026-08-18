# pylint: disable=wildcard-import unused-wildcard-import
from velaw.timesheet_oef_import.config import *
region = 'us-east-1'
instance = 'production'
environment = 'production'
company_key = 'Velaw'
replicon_conn_id = 'velaw_replicon_Rintegrations'
tenant_email = 'Replicon-Firm@Velaw.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

sftp_conn_id = 'sftp_velaw_524663'
input_filepath = '/Authorizerupdate/Production/Input/'
archive_filepath = '/Authorizerupdate/Production/Archive/'
