# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.psa_user_profile_gsap.user_profile_sync.config import *

instance = 'production'
environment = 'production'
company_key = 'dxctechnology'

replicon_conn_id = 'dxctechnology-replicon-RepliconIntPSA'
sftp_conn_id = 'sftp_dxctechnology_628172_PSA'
pgp_conn_id = 'dxctechnology_pgp_psa_user_profiles'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/Production/Inbound/Contractor/Input'
archive_filepath = '/Production/Inbound/Contractor/Archive'
log_filepath = '/Production/Inbound/Contractor/Logs'

can_run_batch_task_var_name = f'dxctechnology_psa_user_import_{instance}_can_run_batch_task'

put_user_service = '/services/ImportService1.svc/PutUser2'
