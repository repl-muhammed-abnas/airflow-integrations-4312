# pylint: disable=wildcard-import unused-wildcard-import
from elevatebiomanagement.time_export.config import *

instance = "production"
environment = 'production'

company_key = 'ElevateBioManagementInc'

export_columns = ["UniqueID","User Name","Employee ID","Client Name","Project Name","Project Code",
                "Task Name","Task Code","Entry Date","Hours",
                "Department (Current)","Home Company (Current)","Cost Center (Current)"]

replicon_conn_id = 'elevatebiomanagementinc_replicon_admin'
http_conn_id = f"elevatebiomanagementinc_anaplan_basic_auth_http_{instance}"
default_http_conn_id = f"elevatebiomanagementinc_http_default_{instance}"
downstream_variable = f"elevatebio_time_export_data_{instance}"
can_run_batch_task_var_name = f"elevatebio_time_export_can_run_batch_task_{instance}"
