# pylint: disable=wildcard-import unused-wildcard-import
from crl.project_import_v1.config import *

instance = "qa"

region = 'us-east-1'
environment = "pre-production"

company_key = "CharlesRiverLaboratoriestrial01"

replicon_conn_id = "CharlesRiverLaboratoriestrial01_replicon_admin"
sftp_conn_id = 'rsftp-useast_for_testing'

log_filepath = '/crl/project/logs/'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

projects_child_dag_id = f'crl_project_import_process_payload_child_{instance}_v1'
client_child_dag_id = f'crl_project_import_process_clients_child_{instance}_v1'
process_project_dag_id = f'crl_project_import_process_each_projects_child_{instance}_v1'
can_run_batch_task_var_name = f'crl_project_import_batch_task_var_{instance}_v1'

disabled=True
