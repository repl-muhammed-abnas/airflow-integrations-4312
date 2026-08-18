region = 'eu-central-1'
environment = "pre-production"

master_max_active_run = 1
max_active_runs_second_child =10
max_active_runs_child=10
execution_timeout_days= 14
parallel_count =5

user_base_report_name = "***User Base Report"
project_report_name ='***project and task details'
task_assignment_report_name = "***Integration Task Assignment Report"
task_assignment_udf_filter_name = 'UDFFilter_Project1_Integration_last_modified_date'

get_project_tenant_log = 'project_and_user_details'
parallel_count= 5

disable_foreign_manager_schedule = "0 0 1 * *"
disable_project_schedule = "0 0 * * SUN"
project_dates_schedule = "0 0 * * *"
log_generation_dag_interval = "0 */3 * * *"
lookup_log_timestamp_hours:int = 3

can_process_payload_var = "wipro_project_task_allocation_can_process_payload"

expected_task_assignment_report_columns = "Project Code,Assignment Start Date,Assignment End Date"
expected_user_report_columns = "Login Name,Employee ID,UserUri"
expected_project_report_columns = "Project Status,Task Status,projectdaydiff,taskdaydiff,ProjectUri,TaskUri"


license_uris = ['urn:replicon-saas:product:psm-enterprise','urn:replicon-saas:product:time-off-enterprise',
                                'urn:replicon-saas:product:wfm-enterprise']

invalid_date = '0000-00-00'
