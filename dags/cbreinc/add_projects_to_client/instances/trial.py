# pylint: disable=wildcard-import unused-wildcard-import
from cbreinc.add_projects_to_client.config import *
region = 'us-east-1'
environment = 'pre-production'
instance = "trial"
company_key = 'CBREIncafmig'
replicon_conn_id = 'cbreincafmig_replicon_Chris.Blade@cbre.com'
can_run_batch_task_var_name = f'cbre_add_project_to_client_{instance}_can_run_batch_task'
schedule_interval = "0 22 * * *"
time_zone = 'America/Chicago'
disabled = True
