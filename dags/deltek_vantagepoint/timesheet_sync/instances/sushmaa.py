from deltek_vantagepoint.initial_setup.instances.sushmaa import oefs, replicon_export_file_format_name, groups
from deltek_vantagepoint.timesheet_sync.config import *
region = 'us-east-1'
environment = 'pre-production'
instance = "sushmaa"
company_key = 'sushmaa'
replicon_conn_id = f'vp_{company_key}_replicon_conn'
deltek_vantagepoint_conn_id = f'vp_{company_key}_vp_conn'
can_run_batch_task_var_name = f'vantagepoint_timesheet_export_can_run_batch_task_{instance}'
post_timesheets_after_sync_var_name = f'vp_post_timesheets_after_sync_{company_key}'
department_name = next(((group['name'].replace(' ', '_') + '_Code') for group in groups if group['id'] == 'homecompany'), None)
allow_lc_update_caption = next((oef['name'].replace(' ', '_') for oef in oefs if oef['id'] == 'allowlcupdate'), None)
workdistribution_caption = next((oef['name'].replace(' ', '_') for oef in oefs if oef['id'] == 'workdistribution'), None)
laborcategorycode_caption = next(((oef['name'].replace(' ', '_') + '__Code_') for oef in oefs if oef['id'] == 'laborcategory'), None)
laborcodelevels = [ (oef['name'].replace(' ','_') + '__Code_') for oef in oefs if oef['id'].startswith('laborcodelevel') ]
