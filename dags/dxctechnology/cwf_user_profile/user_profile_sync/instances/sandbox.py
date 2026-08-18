# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.cwf_user_profile.user_profile_sync.config import *

instance = 'sandbox'
company_key = 'dxcsandbox'

replicon_conn_id = 'dxcsandbox-replicon-RepliconIntFG'
sftp_conn_id = 'dxcsandbox-sftp-628172_fieldglass'
pgp_conn_id = 'dxcsandbox_pgp_cwf_user_profiles'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/Test/Inbound/CWFUserProfiles/Input'
archive_filepath = '/Test/Inbound/CWFUserProfiles/Input/Archives'
log_filepath = '/Test/Inbound/CWFUserProfiles/Logs'

should_add_emailaddress = False

can_run_batch_task_var_name = f'dxctechnology_cwfuserprofiles_{instance}_can_run_batch_task'

put_user_service = '/services/ImportService1.svc/PutUser3'
