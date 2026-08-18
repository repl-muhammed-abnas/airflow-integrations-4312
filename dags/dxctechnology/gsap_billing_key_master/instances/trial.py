# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_billing_key_master.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = 'dxctrial01'
replicon_conn_id = 'dxctrial01-replicon-RepliconIntGSAP'

dag_id_postfix = f'{instance}'

can_run_batch_task_var_name = f'dxctechnology_gsap_billing_key_{instance}_can_run_batch_task'

disable=True

disabled=True
