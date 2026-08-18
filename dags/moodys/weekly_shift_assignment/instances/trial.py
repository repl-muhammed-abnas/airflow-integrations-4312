# pylint: disable=wildcard-import unused-wildcard-import
from moodys.weekly_shift_assignment.config import *

instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'

log_filepath = '/MoodysEMEA/weekly/shiftassignment/logs'
can_run_batch_task_var_name = f'moodys_weekly_shift_assignment_{instance}_can_run_batch_task'
disabled = True
