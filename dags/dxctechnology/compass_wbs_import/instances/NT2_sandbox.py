# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_wbs_import.config import *

input_filepath = '/Test/Inbound/COMPASSWBSMaster/NT2/Processing'
archive_filepath = '/Test/Inbound/COMPASSWBSMaster/NT2/Archive'
log_filepath = '/Test/Inbound/COMPASSWBSMaster/NT2/Logs'
instance = 'NT2_sandbox' #adding _sandbox to retains logs
child_max_active_runs = 100
can_run_batch_task_var_name = f'dxc_compass_wbs_import_{instance}_can_run_batch_task'
