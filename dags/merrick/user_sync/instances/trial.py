# pylint:disable=multiple-statements
from merrick.user_sync.mapper.user_mapper import user_mapper
region = 'us-east-1'
environment = 'pre-production'
instance = "trial"
company_key = 'Merrickandcompanysandbox'
replicon_conn_id = f'polaris_{company_key}'
deltek_cospoint_conn_id = f'deltek_costpoint_{company_key}'
last_run_date_var_name = f'{company_key}_deltek_costpoint_user_sync_last_run_date'
last_run_date_inital_sync_var_name = f'{company_key}_deltek_costpoint_inital_sync_last_run_date'
get_data_in_chunk_var_name = f'{company_key}_deltek_costpoint_user_sync_get_data_in_chunk'
tenant_email = "{{ var.value.dagrun_internal_testing_email }},AvnishKhurana@deltek.com"
internal_email = "{{ var.value.dagrun_internal_testing_email }}"
time_zone = 'America/Denver'
execution_timeout_days = 14
child_dag_max_active_runs = 20
schedule_interval = "*/1 * * * *"
master_dag_interval = 30
can_run_batch_task_var_name = f'merrick_user_import_costpoint_can_run_batch_task_{instance}'
costpoint_date_format = '%Y-%m-%dT%H:%M:%S'
deltek_cospoint_company_ids = ['100','200','500','700']
supervisor_source_field = 'SUPERVISOR'
loginname_source_field = 'EMAIL_ID'
def get_mapper_details(user_param): return do_get_mapper_details(user_param)


def do_get_mapper_details(user_param):
    config_history = user_param['employeehistory'][0] if user_param['employeehistory'] else []
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
lookup_log_timestamp_var = f'{company_key}_deltek_costpoint_import_lookup_log_timestamp_{instance}'
lookup_log_timestamp_hours = 1
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
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
holiday_calendar_from_cp = True
schedule_from_cp = True

is_sso_enabled = True

# DAG IDs
master_dag_id = f'merrick_user_import_costpoint_master_{instance}'
add_user_dag_id = f'merrick_user_import_costpoint_add_user_child_{instance}'
update_user_dag_id = f'merrick_user_import_costpoint_update_user_child_{instance}'
process_each_user_dag_id = f'merrick_user_import_costpoint_process_each_user_child_{instance}'
supervisor_assignment_dag_id = f'merrick_user_import_costpoint_supervisor_assignment_child_{instance}'
create_discipline_roles_dag_id = f'merrick_user_import_costpoint_create_discipline_roles_child_{instance}'
log_generation_dag_id = f'merrick_user_import_costpoint_master_log_scheduled_{instance}'

# Discipline sync specific settings
discipline_last_run_date_var_name = f'{company_key}_discipline_sync_last_run_date'
discipline_can_run_batch_task_var_name = f'{company_key}_discipline_sync_can_run_batch_task'

# Costpoint export filter settings for discipline sync
discipline_employee_filter_id = "polaris_exp_employee_disc"
discipline_employee_rs_id = "EMPL_REF2"
discipline_last_modified_field = "LAST_MODIFIED"
discipline_ref_struc_filter_id = "polaris_exp_ref_element"
discipline_ref_struc_rs_id = "GLMRN_REFSTRUC_HDR"
discipline_ref_id_field = "REF_STRUC_ID"
discipline_ref_desc_field = "REF_STRUC_NAME"
discipline_code_field = "REF2_ID"
discipline_valid_prefixes = ['1DP', '2DP', '5DP', '7DP']

#disabled = True
