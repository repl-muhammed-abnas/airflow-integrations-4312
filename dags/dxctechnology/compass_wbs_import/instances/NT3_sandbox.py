# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_wbs_import.config import *

input_filepath = '/Test/Inbound/COMPASSWBSMaster/NT3/Processing'
archive_filepath = '/Test/Inbound/COMPASSWBSMaster/NT3/Archive'
log_filepath = '/Test/Inbound/COMPASSWBSMaster/NT3/Logs'
instance = 'NT3_sandbox' #adding _sandbox to retains logs
can_run_batch_task_var_name = f'dxc_compass_wbs_import_{instance}_can_run_batch_task'
