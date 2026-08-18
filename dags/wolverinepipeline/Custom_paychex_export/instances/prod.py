# pylint: disable=wildcard-import unused-wildcard-import
from wolverinepipeline.Custom_paychex_export.config import *

region = 'us-east-1'
instance = "prod"
environment = 'production'
company_key = 'WolverinePipeline'

replicon_conn_id = 'WolverinePipeline_replicon_admin'

time_zone = "America/New_York"


filepath = 'WolverinePipeLine/paychex_export/'
archive_filepath = 'WolverinePipeLine/paychex_export/archive/'
log_file_path = 'WolverinePipeLineafmig/paychex_export/'

can_run_batch_task_child = f'wolverinepipeline_user_sync_{instance}_can_run_batch_task'
