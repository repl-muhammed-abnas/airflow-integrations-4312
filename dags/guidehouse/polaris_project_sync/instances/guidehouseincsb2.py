region = 'us-east-1'
environment = 'pre-production'
instance = "sit"

company_key = 'guidehouseincsb2'
replicon_conn_id = 'polaris_guidehouseincsb2'
# deltek_cospoint_conn_id = f'guidehouse_costpoint_polaris_project_sync_{instance}'
deltek_cospoint_conn_id = 'deltek_costpoint_guidehouseincsb2'
time_zone = 'US/Eastern'
execution_timeout_days = 14
child_dag_max_active_runs = 20
date_time_format = "%Y-%m-%dT%H:%M:%S"
schedule_interval = "*/1 * * * *"
master_dag_interval = 3600  # 1 hour
tenant_email = "{{ var.value.dagrun_internal_testing_email }},guidehousedeltekprojectteam@deltek.com,ghcostpoint@guidehouse.com,Timekeeping@guidehouse.com"
internal_email = "{{ var.value.dagrun_internal_testing_email }}"
project_manager_permission_name = 'Project Manager'
deltek_cospoint_company_ids = ['1']
proj_purchase_order_no = 'Purchase Order No'
proj_opportunity_id = 'Opportunity ID'
proj_project_classification = 'Project Classification'
proj_user_company = 'Company'
log_generation_dag_interval = '0 * * * *'
dag_max_active_tasks = 2
lookup_log_timestamp_hours = 1
alert_email = "{{ var.value.dagrun_internal_testing_email }}"
project_leader_approval = True
multi_plc_subtask_mode = True  # False: 1st PLC for user/task is synced | True: Task per PLC created
allow_only_chargeable = False
excluded_project_type_flags = ('E', 'N')
# --- WBS Boundary Sync (Mode 3) ---
enable_wbs_boundary_sync = False
wbs_sync_boundary_level = 2
 
# --- Initial Data Load ---
# Date ranges used by the initial load DAG to fetch projects in batches by
# PJMBASIC_PROJ_LAST_MODIFIED are stored in an Airflow Variable (JSON array),
# not in this config. Set the Variable named below to a JSON array of
# {"from": , "to": } pairs in ISO 8601 format, e.g.
# [{"from": "2026-05-01T00:00:00.000000+00:00", "to": "2026-06-01T00:00:00.000000+00:00"}]
# If the Variable is not set, the initial load DAG processes no date ranges.
initial_load_date_ranges_var_name = f'guidehouse_costpoint_polaris_project_initial_load_date_ranges_{instance}'
assign_allusers_on_update = True

# Number of taskHierarchy entries sent per CreateTaskHierarchyOrApplyModifications call in update_task
task_hierarchy_batch_size = 20

workforce_change_detection_enabled = True

is_project_role_assigment_enabled = True

proj_source_name = "Source System"

exclude_project_type_prefix = "LEAVE"

parallel_count = 15
proj_project_type = "Project Type"
task_type_custom_field = "Task Type"
proj_source_system = "Source System"
proj_service_center_name = "CostPoint"
cp_role_to_pm_map = {
    'LEM': 'pm',
    'BL': 'co',
    'LED': 'co',
    'LEP': 'co',
    'PFD': 'co',
    'SUPPORTD': 'co',
    'SUPPORTP': 'co',
    'PA': 'co',
}

main_dag_id = f'guidehouseinc_costpoint_polaris_project_sync_main_{instance}'
initial_load_dag_id = f'guidehouseinc_costpoint_polaris_project_initial_load_main_{instance}'
child_dag_id = f'guidehouseinc_costpoint_polaris_replicon_endpoint_caller_{instance}'
replicon_endpoint_caller_dag_id = f'guidehouseinc_polaris_replicon_endpoint_caller_{instance}'
last_run_date_var_name = f'guidehouse_costpoint_polaris_project_sync_last_run_date_{instance}'
lookup_log_timestamp_var = f'guidehouse_costpoint_polaris_project_sync_lookup_log_timestamp_{instance}'
get_data_in_chunk_var_name = f'guidehouse_costpoint_polaris_project_sync_get_data_in_chunk_{instance}'
can_run_batch_task_var_name = f'guidehouse_costpoint_polaris_project_sync_run_batch_task_{instance}'
project_chunk_number_var_name = f'guidehouse_costpoint_polaris_project_sync_project_chunk_number_{instance}'
bulk_update_inline_threshold_var_name = f'guidehouse_costpoint_polaris_project_sync_bulk_update_inline_threshold_{instance}'
assign_task_resources_inline_threshold_var_name = f'guidehouse_costpoint_polaris_project_sync_assign_task_resources_inline_threshold_{instance}'
filter_active_projects_only_var_name = f'guidehouse_costpoint_polaris_project_sync_filter_active_projects_only_{instance}'
