region = 'us-east-1'
environment = 'qa'
instance = "cppsawbs2"
company_key = 'cppsawbs2'
replicon_conn_id = f'polaris_{company_key}'
deltek_cospoint_conn_id =  f'deltek_costpoint_{company_key}'
last_run_date_var_name = f'{company_key}_deltek_costpoint_polaris_project_sync_last_run_date'
get_data_in_chunk_var_name = f'{company_key}_deltek_costpoint_project_sync_get_data_in_chunk'
time_zone = 'US/Eastern'
execution_timeout_days = 14
child_dag_max_active_runs = 2
date_time_format = "%Y-%m-%dT%H:%M:%S"
can_run_batch_task_var_name = f'{company_key}_deltek_costpoint_project_sync_run_batch_task'
schedule_interval = "*/1 * * * *"
master_dag_interval = 300000
tenant_email = "MPTeamReplicon@deltek.com"
internal_email = "MPTeamReplicon@deltek.com"
project_manager_permission_name = 'Project Manager'
# 1 Applied Technologies Inc
# 04 British, Co.
# 6 Manufacturing, Co.
# 5 Nonprofit, Co.
# 3 Spanish, Co.
# 2 USA, Co.
deltek_cospoint_company_ids = ['1', '04', '6', '5', '3', '2']
proj_purchase_order_no = 'Purchase Order No'
proj_opportunity_id = 'Opportunity ID'
proj_project_classification = 'Project Classification'
proj_user_company = 'Company'
log_generation_dag_interval = '0 * * * *'
dag_max_active_tasks = 2
lookup_log_timestamp_var = f'deltek_costpoint_project_import_{instance}_lookup_log_timestamp'
lookup_log_timestamp_hours = 1
alert_email = "MPTeamReplicon@deltek.com"
project_leader_approval = True
deltek_cospoint_sql_conn_id = "RIA_ODBC"
proj_source_name = "Source System"
allow_only_chargeable = True # If True, only chargeable projects will be synced. If False, all projects will be synced.
excluded_project_type_flags = ('E', 'N')

# --- WBS Boundary Sync (Mode 2) ---
enable_wbs_boundary_sync = False #True: Sync WBS up to boundary level defined below for projects that have changed since last sync
wbs_sync_boundary_level = 3
multi_plc_subtask_mode = True #False 1st PLC for user/task is synced #True: Task for PLC created

# --- Initial Data Load ---
# Date ranges used by the initial load DAG to fetch projects in batches by
# PJMBASIC_PROJ_LAST_MODIFIED are stored in an Airflow Variable (JSON array),
# not in this config. Set the Variable named below to a JSON array of
# {"from": , "to": } pairs in ISO 8601 format, e.g.
# [{"from": "2026-05-01T00:00:00.000000+00:00", "to": "2026-06-01T00:00:00.000000+00:00"}]
# If the Variable is not set, the initial load DAG processes no date ranges.
initial_load_date_ranges_var_name = f'{company_key}_deltek_costpoint_polaris_project_initial_load_date_ranges'
assign_allusers_on_update = True

# Number of taskHierarchy entries sent per CreateTaskHierarchyOrApplyModifications call in update_task
task_hierarchy_batch_size = 3

workforce_change_detection_enabled = False

is_project_role_assigment_enabled = False

CP_ROLE_TO_PM_MAP = {
    'PM': 'pm',
    'BL': 'co',
    'CL': 'co',
    'PBA': 'co',
    'PROPM': 'co',
    'PCO': 'co'
}

project_type_exclusions = ['B&P']
project_classification_exclusions = ['INTER-COMPANY', 'INTER-CO MULTI']
require_chargeable_leaf_in_hierarchy = False
