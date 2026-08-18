# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.project_import_ury_and_arg.config import *

instance = 'dev'
environment = 'pre-production'

company_key = 'pwcdev'
replicon_conn_id = 'pwcdev-replicon-eu.automation'

sftp_conn_id = 'pwcglobaldev-MFT-STG-replicon'

input_filepath = '/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Project/SDC/LAN'
log_filepath = '/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Project/_logs'
archive_filepath = '/PwCGBL_RepliconGlobal_STG/DEV/Inbound/Project/SDC/LAN/_archive'

tenant_email = 'sdc_xb_rm_team@pwc.com,xb_innovation_automation_team@pwc.com,us_replicondevextintegrationalerts@pwc.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task = f'pwc_project_import_filebased_{instance}_can_run_batch_task'

master = f"pwc_project_import_ury_and_arg_master_{instance}"
process_clients = f"pwc_project_import_ury_and_arg_process_clients_child_{instance}"
process_projects = f"pwc_project_import_ury_and_arg_process_projects_child_{instance}"
process_log_generation = f"pwc_project_import_ury_and_arg_process_log_generation_child_{instance}"
project_belongs_to = "AC Integrado"
