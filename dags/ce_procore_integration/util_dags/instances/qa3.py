# pylint: disable=wildcard-import unused-wildcard-import
from ce_procore_integration.util_dags.config import *

instance = 'qa3'
environment = 'qa'

# Connection IDs
procore_conn_id = f'procore_{instance}'

wbs_code_creator_dag_id = f'computerease_procore_wbs_code_creator_{instance}'
