# pylint:disable=multiple-statements
from deltek_costpoint_polaris.user_sync.instances.mapper.cppsawbs2_mapper import user_mapper
from deltek_costpoint_polaris.user_sync.utils.user_criteria_filter import matches_mapper, find_mapper_row
region = 'us-east-1'
environment = 'qa'
instance = "cppsawbs2"
company_key = 'cppsawbs2'
replicon_conn_id = f'polaris_{company_key}'
deltek_cospoint_conn_id = f'deltek_costpoint_{company_key}'
last_run_date_var_name = f'{company_key}_deltek_costpoint_user_sync_last_run_date'
last_run_date_inital_sync_var_name = f'{company_key}_deltek_costpoint_inital_sync_last_run_date'
get_data_in_chunk_var_name = f'{company_key}_deltek_costpoint_user_sync_get_data_in_chunk'
time_zone = 'US/Eastern'
tenant_email = "MPTeamReplicon@deltek.com"
internal_email = "MPTeamReplicon@deltek.com"
execution_timeout_days = 14
child_dag_max_active_runs = 2
schedule_interval = "*/1 * * * *"
master_dag_interval = 30
can_run_batch_task_var_name = f'Costpoint_user_import_can_run_batch_task_{instance}'
costpoint_date_format = '%Y-%m-%dT%H:%M:%S'
deltek_cospoint_company_ids = ['1', '04', '6', '5', '3', '2']
supervisor_source_field = 'MANAGER'
loginname_source_field = 'EMPL_ID'
def get_mapper_details(user_param): return do_get_mapper_details(user_param)


def matches_filter_criteria(user_param): return matches_mapper(user_param, user_mapper)


def do_get_mapper_details(user_param): return find_mapper_row(user_param, user_mapper)


log_generation_dag_interval = '0 * * * *'
dag_max_active_tasks = 2
lookup_log_timestamp_var = f'deltek_costpoint_import_{instance}_lookup_log_timestamp'
lookup_log_timestamp_hours = 1
internal_logs_email = "MPTeamReplicon@deltek.com"
alert_email = "MPTeamReplicon@deltek.com"
child_dag_log_generation_max_active_runs = 2
oef_generallabourcategories = "General labour categories"
oef_paytype = "Pay Type"
oef_oeftaxableentity = "Taxable Entity"
oef_oefemployeeclass = "Employee Class"
oef_oefflsaexempt = "FLSA Exempt"
oef_projectlaborcategory = "Project Labor Category"
oef_company = "Company"
oef_dtype = "Id Type"
costpoint_timeoff_type_name = "Time Off"
holiday_calendar_from_cp = False
schedule_from_cp = False
is_sso_enabled = False
