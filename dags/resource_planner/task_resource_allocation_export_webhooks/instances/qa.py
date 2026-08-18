# pylint: disable=wildcard-import unused-wildcard-import
from resource_planner.task_resource_allocation_export_webhooks.config import *
from datetime import datetime

instance = "qa"
region = 'us-east-1'
environment = 'pre-production'

# Replicon configuration
company_key = 'Repliconpincstream6dev'
replicon_conn_id = 'replicon_Repliconpincstream6dev_replicon'

# RP Backend API connection
rp_api_conn_id = 'resource_planning_api_connection'
rp_api_db_env = "qa"
rp_api_target_table = None  # Use production table

# DAG configuration
start_date = datetime(2025, 1, 1)

# DAG IDs
new_allocation_dag_id = f"resource_planner_task_alloc_webhook_new_{instance}"
modified_allocation_child_dag_id = f"resource_planner_task_alloc_webhook_modified_child_{instance}"
deleted_allocation_dag_id = f"resource_planner_task_alloc_webhook_deleted_{instance}"
