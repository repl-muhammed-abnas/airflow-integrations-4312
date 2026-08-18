# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.custom_import_for_teammanager_permission_v1.config import *
instance = 'prod'
region = 'eu-central-1'
environment = 'production'

company_key = 'pwc'

replicon_conn_id = 'pwcglobal-replicon-eu.automation'

sftp_conn_id = 'pwcglobal-MFT-PRD-replicon'

country_code = 'JPN'

input_filepath = '/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Staff/Local/JP/Sup_Org_Asgmt'
log_filepath = '/PwCGBL_RepliconGlobal_PRD/PRD/Inbound/Staff/Local/JP/_logs'

tenant_email = ['bartosz.polawski@pwc.com','PWCGlobalLogs@deltek.com']
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'pwc_custom_import_for_teammanager_permission_{instance}_can_run_batch_task'

version = 'v1'

master_dagid = f"pwc_custom_import_for_teammanager_permission_master_{instance}_{version}"
process_supervisory_org_permission_assignment_child = f"pwc_custom_import_for_teammanager_permission_supervisory_org_assignment_child_{instance}_{version}"
process_log_generation = f"pwc_custom_import_for_teammanager_permission_log_generation_child_{instance}_{version}"
