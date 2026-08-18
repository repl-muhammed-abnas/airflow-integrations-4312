# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.psa_user_profile_gsap.user_profile_sync.config import *

instance = 'trial'
company_key = 'dxctrial01'

replicon_conn_id = 'dxctrial01'
sftp_conn_id = 'rsftp-useast_for_testing'
pgp_conn_id = 'dxctrial01_pgp_cwf_user_profiles'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/DXC/C1WBS/input'
archive_filepath = '/DXC/C1WBS/archive'
log_filepath = '/DXC/C1WBS/logs'
can_run_batch_task_var_name = f'dxctechnology_psa_user_import_{instance}_can_run_batch_task'

put_user_service = '/services/ImportService1.svc/PutUser3'

disable = True

disabled = True
