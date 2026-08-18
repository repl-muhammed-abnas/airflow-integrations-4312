# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_wbs_import.config import *

input_filepath = '/Test/Inbound/COMPASSWBSMaster/NT4/Processing'
archive_filepath = '/Test/Inbound/COMPASSWBSMaster/NT4/Archive'
log_filepath = '/Test/Inbound/COMPASSWBSMaster/NT4/Logs'
instance = 'NT4_sandbox' #adding _sandbox to retains logs
can_run_batch_task_var_name = f'dxc_compass_wbs_import_{instance}_can_run_batch_task'
