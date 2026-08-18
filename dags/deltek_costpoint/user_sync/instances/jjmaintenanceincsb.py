# pylint:disable=multiple-statements
from deltek_costpoint.user_sync.instances.mapper.user_mapper import user_mapper
region = 'us-east-1'
environment = 'pre-production'
instance = "JJMaintenanceIncSB"
company_key = 'JJMaintenanceIncSB'
replicon_conn_id = f'replicon_{company_key}'
deltek_cospoint_conn_id = f'deltek_costpoint_{company_key}'
last_run_date_var_name = f'{company_key}_deltek_costpoint_user_sync_last_run_date'
get_data_in_chunk_var_name = f'{company_key}_deltek_costpoint_user_sync_get_data_in_chunk'
time_zone = 'US/Eastern'
tenant_email = "MPTeamReplicon@deltek.com"
internal_email = "MPTeamReplicon@deltek.com"
time_zone = 'US/Eastern'
execution_timeout_days = 14
child_dag_max_active_runs = 2
schedule_interval = "*/1 * * * *"
master_dag_interval = 30
can_run_batch_task_var_name = f'Cospoint_user_import_can_run_batch_task_{instance}'
costpoint_date_format = '%Y-%m-%dT%H:%M:%S'
deltek_cospoint_company_ids = ['1']
def get_mapper_details(user_param): return do_get_mapper_details(user_param)

def do_get_mapper_details(user_param):
    config_history = user_param['employeehistory'][0] if user_param['employeehistory'] else [
    ]
    if config_history:
        cinfiguration_field = list(filter(lambda x: user_param['costcenter'] and x['account'] == user_param['costcenter'].strip()
                                          and user_param['servicecenter'] and x['country'] == user_param['servicecenter'].strip()
                                          and config_history['location'] and x['labor_location'] == config_history['location'].strip()
                                          and config_history['division'] and x['organizations'] == config_history['division'].strip()
                                          and config_history['generalLabercategory'] and x['general_labor_category'] == config_history['generalLabercategory'].strip()
                                          and config_history['employeetype'] and x['employee_type'] == config_history['employeetype'].strip(), user_mapper))
        return cinfiguration_field[0] if cinfiguration_field else user_mapper[0]
    return user_mapper[0] if user_mapper else None


log_generation_dag_interval = '0 * * * *'
dag_max_active_tasks = 2
lookup_log_timestamp_var = f'deltek_costpoint_import_{instance}_lookup_log_timestamp'
lookup_log_timestamp_hours = 1
internal_logs_email = "MPTeamReplicon@deltek.com"
alert_email = "MPTeamReplicon@deltek.com"
child_dag_log_generation_max_active_runs = 20
oef_generallabourcategories = "General labour categories"
oef_paytype = "Pay Type"
oef_oeftaxableentity = "Taxable Entity"
oef_oefemployeeclass = "Employee Class"
oef_oefflsaexempt = "FLSA Exempt"
oef_projectlaborcategory = "Project Labor Category"
oef_company = "Company"
oef_dtype = "Id Type"
costpoint_timeoff_type_name = "Time Off"
holiday_calendar_from_cp = True
schedule_from_cp = True
