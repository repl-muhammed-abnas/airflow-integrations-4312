# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.psa_user_profile_gsap.user_profile_sync.config import *

environment = 'pre-production'

instance = "sandbox"

company_key = 'dxcsandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntPSA'
sftp_conn_id = 'sftp_dxctechnology_psa'
pgp_conn_id = 'dxcsandbox_pgp_psa_user_profiles'

input_filepath = '/Test/Inbound/Contractor/Input'
archive_filepath = '/Test/Inbound/Contractor/Archive'
log_filepath = '/Test/Inbound/Contractor/Logs'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
can_run_batch_task_var_name = f'dxctechnology_psa_user_import_{instance}_can_run_batch_task'

put_user_service = '/services/ImportService1.svc/PutUser3'
