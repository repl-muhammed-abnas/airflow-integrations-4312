# pylint: disable=wildcard-import unused-wildcard-import
from resource_planner.task_resource_allocation_export_webhooks.config import *
from datetime import datetime

instance = "test"
region = 'us-east-1'
environment = 'production'

# Replicon configuration
company_key = 'RepliconPIncStream6UAT'
replicon_conn_id = 'replicon_RepliconPIncStream6UAT_resourceplannertool.integration'

# RP Backend API connection
rp_api_conn_id = 'resource_planning_api_connection'
rp_api_db_env = "test"
rp_api_target_table = None  # Routes to test DB via rp_api_db_env="test"

# DAG configuration
start_date = datetime(2025, 1, 1)

# DAG IDs
new_allocation_dag_id = f"resource_planner_task_alloc_webhook_new_{instance}"
modified_allocation_child_dag_id = f"resource_planner_task_alloc_webhook_modified_child_{instance}"
deleted_allocation_dag_id = f"resource_planner_task_alloc_webhook_deleted_{instance}"
