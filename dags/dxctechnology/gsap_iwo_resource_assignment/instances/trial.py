# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_iwo_resource_assignment.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = 'dxctrial01'
replicon_conn_id = 'dxctrial01'

dag_id_postfix = f'{instance}'

can_run_batch_task_var_name = f'dxctechnology_gsap_iwo_resource_{instance}_can_run_batch_task'
