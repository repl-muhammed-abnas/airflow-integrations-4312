# pylint: disable=wildcard-import unused-wildcard-import
from kla.disable_cost_center_dept.config import *

region = 'us-east-2'
instance = "production"
environment = 'production'
company_key = 'kla'
replicon_conn_id = 'KLA-replicon-RNadmin'
http_conn_id = f"kla_costcenter_dept_disable_{instance}"
schedule_interval = "0 9 * * SUN"
can_run_batch_task_var_name = f"kla_costcenter_dept_disable_{instance}_can_run_batch_task"
