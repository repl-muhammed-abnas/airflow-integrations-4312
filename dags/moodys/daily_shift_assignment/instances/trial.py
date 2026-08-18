# pylint: disable=wildcard-import unused-wildcard-import
from moodys.daily_shift_assignment.config import *

instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'

log_filepath = '/MoodysEMEA/daily/shiftassignment/logs'
can_run_batch_task_var_name = f'moodys_daily_shift_assignment_{instance}_can_run_batch_task'

reference_filepath = '/MoodysEMEA/daily/shiftassignment/reference'

disabled=True
