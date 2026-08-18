# pylint: disable=wildcard-import unused-wildcard-import
from crl.project_import_v2.config import *

instance = "prod"

region = 'us-east-1'
environment = "production"

company_key = "CharlesRiverLaboratories"

replicon_conn_id = "CharlesRiverLaboratories_repliconint_projectimport"

sftp_conn_id = 'sftp_charlesriverlaboratories_603355'

log_filepath = '/Production/Inbound/Project Import/Logs/'

tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com,MTL-Payroll@crl.com,Shari.Guttman@crl.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

projects_child_dag_id = f'crl_project_import_process_payload_child_{instance}_v2'
client_child_dag_id = f'crl_project_import_process_clients_child_{instance}_v2'
process_project_dag_id = f'crl_project_import_process_each_projects_child_{instance}_v2'
can_run_batch_task_var_name = f'crl_project_import_batch_task_var_{instance}_v2'
