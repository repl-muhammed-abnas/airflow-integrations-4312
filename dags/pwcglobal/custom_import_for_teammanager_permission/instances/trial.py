# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.custom_import_for_teammanager_permission.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'pwcinternal'
replicon_conn_id = 'pwcinternal-replicon-eu.automation'

sftp_conn_id = 'sftp_eucentral1_airflow'

country_code = 'JPN'

input_filepath = '/PwCGlobal/custom_import_for_teammanager_permission/input'
log_filepath = '/PwCGlobal/custom_import_for_teammanager_permission/logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'pwc_custom_import_for_teammanager_permission_{instance}_can_run_batch_task'

master_dagid = f"pwc_custom_import_for_teammanager_permission_master_{instance}"
process_supervisory_org_permission_assignment_child = f"pwc_custom_import_for_teammanager_permission_supervisory_org_assignment_child_{instance}"
process_log_generation = f"pwc_custom_import_for_teammanager_permission_log_generation_child_{instance}"
disabled = True
