# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_task_import_project_fields.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = "trial"
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-replicon-RepliconIntGSAP'

can_run_batch_task_var_name = f'dxctechnology_gsap_task_import_project_fields_{instance}_can_run_batch_task'
can_run_batch_task_var_name_child_dag = f'dxctechnology_gsap_task_import_project_fields_{instance}_can_run_batch_task_child_dag'
disabled = True
