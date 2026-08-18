# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.report_batch_processing.config import *

instance = "sandbox2"
company_key = 'DXCSandbox2'
replicon_conn_id = 'DXCSandbox2_Replicon_Report.APIUser'
bearer_token_var = f'dxc_report_batch_processing_{instance}_token'

can_run_batch_task_var_name = f'dxc_report_batch_processing_{instance}_can_run_batch_task'
