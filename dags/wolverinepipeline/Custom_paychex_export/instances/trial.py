# pylint: disable=wildcard-import unused-wildcard-import
from wolverinepipeline.Custom_paychex_export.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'WolverinePipelineafmig'

replicon_conn_id = 'WolverinePipelineafmig_replicon_admin'

time_zone = "America/New_York"


filepath = 'WolverinePipeLineafmig/paychex_export/'
archive_filepath = 'WolverinePipeLineafmig/paychex_export/paychex_export/archive/'
log_file_path = 'WolverinePipeLineafmig/paychex_export/'

can_run_batch_task_child = f'wolverinepipeline_user_sync_{instance}_can_run_batch_task'
