# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_wbs_import.config import *

input_filepath = '/Production/Inbound/COMPASSWBSMaster/PPC/Processing'
archive_filepath = '/Production/Inbound/COMPASSWBSMaster/PPC/Archive'
log_filepath = '/Production/Inbound/COMPASSWBSMaster/PPC/Logs'
instance = 'PPC_DXCTechnology'
environment = 'production'
company_key = 'DXCTechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntCompass'
sftp_conn_id = 'sftp_dxctechnology_compass'
can_run_batch_task_var_name = f'dxc_compass_wbs_import_{instance}_can_run_batch_task'
