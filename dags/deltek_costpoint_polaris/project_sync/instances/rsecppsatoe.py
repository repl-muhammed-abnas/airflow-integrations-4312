region = 'us-east-1'
environment = 'qa'
instance = "rsecppsatoe"
company_key = 'rsecppsatoe'
replicon_conn_id = f'polaris_{company_key}'
deltek_cospoint_conn_id = f'deltek_costpoint_{company_key}'
last_run_date_var_name = f'{company_key}_deltek_costpoint_project_sync_last_run_date'
get_data_in_chunk_var_name = f'{company_key}_deltek_costpoint_project_sync_get_data_in_chunk'
time_zone = 'America/New_York'
execution_timeout_days = 14
child_dag_max_active_runs = 2
date_time_format = "%Y-%m-%dT%H:%M:%S"
can_run_batch_task_var_name = f'{company_key}_deltek_costpoint_project_sync_run_batch_task'
schedule_interval = "*/1 * * * *"
master_dag_interval = 30
tenant_email = "MPTeamReplicon@deltek.com,ShawnKlies@deltek.com"
internal_email = "MPTeamReplicon@deltek.com"
project_manager_permission_name = 'Project Manager'
# 1 Applied Technologies Inc
# 04 British, Co.
# 6 Manufacturing, Co.
# 5 Nonprofit, Co.
# 3 Spanish, Co.
# 2 USA, Co.
deltek_cospoint_company_ids = ['1']
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