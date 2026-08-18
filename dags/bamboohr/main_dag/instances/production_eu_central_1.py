# pylint: disable=wildcard-import unused-wildcard-import
from bamboohr.main_dag.config import *

instance = 'production'

region = 'eu-central-1'
environment = 'production'
company_key = f"airflow{region.replace('-', '')}"
replicon_conn_id = 'airflow-replicon-admin'

timezone_iana = 'Europe/Paris'

can_run_batch_task_var_name = f'standard_bamboohr_main_dag_{instance}_can_run_batch_task'

user_import_dag = f"standard_bamboohr_{region.replace('-', '_')}_user_import_{instance}"
disable_user_dag = f"standard_bamboohr_{region.replace('-', '_')}_disable_user_{instance}"
