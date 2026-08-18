# pylint: disable=wildcard-import unused-wildcard-import
from ce_procore_integration.util_dags.config import *

instance = "dev"

# Connection IDs for trial environment
procore_conn_id = f'procore_{instance}'

wbs_code_creator_dag_id = f'computerease_procore_wbs_code_creator_{instance}'
