# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.psa_resource_assignment_v1.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = 'dxctrial01'
replicon_conn_id = 'dxctrial01'

dag_id_postfix = f'{instance}'

pgp_conn_id = 'pgp_dxcsandbox_psa_resource_assignment'

can_run_batch_task_var_name = f'dxctechnology_psa_resource_{instance}_can_run_batch_task_v1'
can_decrypt_file_var_name = f'dxctechnology_psa_resource_{instance}_can_decrypt_file_v1'
