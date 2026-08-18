# pylint: disable=wildcard-import unused-wildcard-import
from lanter_delivery_systems.user_import.disable_user_integration.config import *

instance = 'uat'
environment = "pre-production"

company_key = 'ldstrial01'
replicon_conn_id = 'ldstrial01_replicon_admin'
sftp_conn_id = "replicon_sftp_lds_676481"

input_filepath = "/Trial/UserImport/DisabledUsers"
archive_filepath = "/Trial/UserImport/Archive"
log_filepath = "/Trial/UserImport/LogFile"

tenant_email = "Jacob.Grass@rubinbrown.com"
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'lds_user_import_disable_user_master_{instance}'
child_dagid = f'lds_user_import_disable_user_child_{instance}'

disable=True

disabled=True
