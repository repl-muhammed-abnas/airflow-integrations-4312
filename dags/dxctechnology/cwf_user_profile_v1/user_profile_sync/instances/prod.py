# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.cwf_user_profile_v1.user_profile_sync.config import *

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

cwf_main_dagid = f'dxctechnology_cwf_userprofiles_master_{instance}_v1'
cwf_add_userprofiles_dagid = f'dxctechnology_cwf_userprofiles_add_child_{instance}_v1'
cwf_update_userprofiles_dagid = f'dxctechnology_cwf_userprofiles_update_child_{instance}_v1'
cwf_supervisor_userprofiles_dagid = f'dxctechnology_cwf_userprofiles_supervisor_child_{instance}_v1'
cwf_log_userprofiles_dagid = f'dxctechnology_cwf_userprofiles_log_child_{instance}_v1'
cwf_process_userprofiles_dagid = f'dxctechnology_cwf_userprofiles_child_{instance}_v1'

gsap_main_dagid = f'dxctechnology_cwf_gsap_userprofiles_master_{instance}_v1'
gsap_add_userprofiles_dagid = f'dxctechnology_cwf_gsap_userprofiles_add_child_{instance}_v1'
gsap_update_userprofiles_dagid = f'dxctechnology_cwf_gsap_userprofiles_update_child_{instance}_v1'
gsap_process_userprofiles_dagid = f'dxctechnology_cwf_gsap_userprofiles_child_{instance}_v1'

put_user_service = '/services/ImportService1.svc/PutUser2'
