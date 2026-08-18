# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.supervisor_org_group_import.config import *

instance = 'pwcdev'
environment = 'pre-production'

company_key = 'pwcdev'
replicon_conn_id = 'pwcdev-replicon-eu.automation'

sftp_conn_id = 'pwcglobaldev-MFT-STG-replicon'

input_filepath = '/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Staff/Local/JP/Sup_Org'
log_filepath = '/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Staff/Local/JP/_logs'
archive_filepath = '/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Staff/Local/JP/_archive'

tenant_email = 'bartosz.polawski@pwc.com,us_replicondevextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task = f'pwc_supervisor_org_group_{instance}_can_run_batch_task'

master_dagid = f"pwc_supervisor_org_group_import_master_{instance}"
add_dagid = f"pwc_supervisor_org_group_import_add_child_{instance}"
disable_dagid = f"pwc_supervisor_org_group_import_disable_child_{instance}"
process_log_generation = f"pwc_supervisor_org_group_import_log_generation_child_{instance}"

disabled=True
