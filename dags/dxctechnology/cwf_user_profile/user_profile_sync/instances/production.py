# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.cwf_user_profile.user_profile_sync.config import *

instance = 'production'
environment = 'production'
company_key = 'dxctechnology'

replicon_conn_id = 'DXCTechnology_http_RepliconIntFG'
sftp_conn_id = 'dxctechnology_sftp_628172_fieldglass'
pgp_conn_id = 'dxctechnology_pgp_cwf_user_profiles'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = '/Production/Inbound/CWFUserProfiles/Input'
archive_filepath = '/Production/Inbound/CWFUserProfiles/Archive'
log_filepath = '/Production/Inbound/CWFUserProfiles/Logs'

can_run_batch_task_var_name = f'dxctechnology_cwfuserprofiles_{instance}_can_run_batch_task'

put_user_service = '/services/ImportService1.svc/PutUser2'
