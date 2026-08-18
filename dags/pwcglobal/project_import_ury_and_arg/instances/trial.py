# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.project_import_ury_and_arg.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'pwcinternal'
replicon_conn_id = 'pwcinternal-replicon-eu.automation'

sftp_conn_id = 'sftp_internal'

input_filepath = '/PwCGlobal/project_import_ury_and_arg/input'
log_filepath = '/PwCGlobal/project_import_ury_and_arg/logs'
archive_filepath = '/PwCGlobal/project_import_ury_and_arg/archive'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task = f'pwc_project_import_filebased_{instance}_can_run_batch_task'

master = f"pwc_project_import_ury_and_arg_master_{instance}"
process_clients = f"pwc_project_import_ury_and_arg_process_clients_child_{instance}"
process_projects = f"pwc_project_import_ury_and_arg_process_projects_child_{instance}"
process_log_generation = f"pwc_project_import_ury_and_arg_process_log_generation_child_{instance}"
project_belongs_to = "AC Integrado"
disabled = True
