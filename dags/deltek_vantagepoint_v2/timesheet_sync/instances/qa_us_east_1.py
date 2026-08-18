from deltek_vantagepoint_v2.initial_setup.instances.qa_us_east_1 import oefs, replicon_export_file_format_name, groups
from deltek_vantagepoint_v2.timesheet_sync.config import *

region = 'us-east-1'
environment = 'qa'
instance = 'qa'
company_key = f"airflowqasandbox{region.replace('-', '')}"
replicon_conn_id = 'airflowqasandbox-replicon-admin'

can_run_batch_task_var_name = f'vp_replicon_timesheet_export_can_run_batch_task_{instance}'
timecategory_sync_last_run_var = f'vp_replicon_timecategory_sync_last_run_{instance}'
post_timesheets_after_sync_var_name = f'vp_replicon_post_timesheets_after_sync_{instance}'

department_name = next(((group['name'].replace(' ', '_') + '_Code') for group in groups if group['id'] == 'homecompany'), None)
allow_lc_update_caption = next((oef['name'].replace(' ', '_') for oef in oefs if oef['id'] == 'allowlcupdate'), None)
workdistribution_caption = next((oef['name'].replace(' ', '_') for oef in oefs if oef['id'] == 'workdistribution'), None)
laborcategorycode_caption = next(((oef['name'].replace(' ', '_') + '__Code_') for oef in oefs if oef['id'] == 'laborcategory'), None)
laborcodelevels = [ (oef['name'].replace(' ','_') + '__Code_') for oef in oefs if oef['id'].startswith('laborcodelevel') ]

should_post_timeentry_comments = True

# DAG IDs
timesheet_sync_main_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_timesheet_sync_main_{instance}'
timesheet_per_company_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_timesheet_sync_foreach_company_child_{instance}'
timesheet_for_employee_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_timesheet_sync_for_employee_child_{instance}'
timesheet_for_employee_per_period_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_timesheet_sync_for_employee_per_period_child_{instance}'
timecategory_sync_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_user_timecategory_sync_{instance}'
timecategory_sync_child_dag_id = f'standard_deltek_vantagepoint_{region.replace("-", "_")}_user_timecategory_sync_child_{instance}'

# History logging configs
provider = 'vantagepoint'
workflow = 'timesheet_sync'
airflow_connector_ui_connid = 'airflow_connector_ui_endpoint_vantagepoint'
hmac_secret = 'airflow_connector_ui_hmac_secret'

timesheet_field_oef_name_for_lc = 'Labor Codes'
enable_budget_labor_codes_level = True
budget_labor_codes_level = "TimesheetFields" # Task / TimesheetFields
