# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.supervisor_org_group_import.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'pwcinternal'
replicon_conn_id = 'pwcinternal-replicon-eu.automation'

sftp_conn_id = 'sftp_internal_integration_useast'

input_filepath = '/PwCGlobal/supervisor_org_group_import/input'
log_filepath = '/PwCGlobal/supervisor_org_group_import/logs'
archive_filepath = '/PwCGlobal/supervisor_org_group_import/archive'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task = f'pwc_supervisor_org_group_{instance}_can_run_batch_task'

master_dagid = f"pwc_supervisor_org_group_import_master_{instance}"
add_dagid = f"pwc_supervisor_org_group_import_add_child_{instance}"
disable_dagid = f"pwc_supervisor_org_group_import_disable_child_{instance}"
process_log_generation = f"pwc_supervisor_org_group_import_log_generation_child_{instance}"
disabled=True
