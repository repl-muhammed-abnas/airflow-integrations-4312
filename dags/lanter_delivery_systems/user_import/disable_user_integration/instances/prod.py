# pylint: disable=wildcard-import unused-wildcard-import
from lanter_delivery_systems.user_import.disable_user_integration.config import *

instance = 'production'
environment = "production"

company_key = 'lds'
replicon_conn_id = 'lds_replicon_admin'
sftp_conn_id = "replicon_sftp_lds_676481"

input_filepath = "/Production/UserImport/DisabledUsers"
archive_filepath = "/Production/UserImport/Archive"
log_filepath = "/Production/UserImport/LogFile"

tenant_email = "Jacob.Grass@rubinbrown.com,AWelden@lanterds.com,rstone@ctr.lanterds.com,hr@lanterds.com,recruiting@lanterds.com"
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'lds_user_import_disable_user_master_{instance}'
child_dagid = f'lds_user_import_disable_user_child_{instance}'
