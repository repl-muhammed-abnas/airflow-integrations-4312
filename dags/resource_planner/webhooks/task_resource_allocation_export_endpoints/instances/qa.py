# pylint: disable=wildcard-import unused-wildcard-import
from resource_planner.webhooks.task_resource_allocation_export_endpoints.config import *

instance = "qa"
processor_instance = "qa"

environment = "pre-production"
rp_api_db_env = "qa"

company_key = "Repliconpincstream6dev"

replicon_conn_id = "replicon_Repliconpincstream6dev_replicon"

user_import_master_dag_id = f"resource_planner_task_resource_allocation_master_webhook_{instance}"
user_import_process_payload_child_dag_id = f"resource_planner_task_resource_allocation_process_payload_child_{instance}"

# Webhook receiver DAG IDs (one per event type)
webhook_created_dag_id = f"resource_planner_task_alloc_webhook_created_{instance}"
webhook_modified_dag_id = f"resource_planner_task_alloc_webhook_modified_{instance}"
webhook_deleted_dag_id = f"resource_planner_task_alloc_webhook_deleted_receiver_{instance}"

# Target processing DAG IDs
new_allocation_dag_id = f"resource_planner_task_alloc_webhook_new_{processor_instance}"
modified_allocation_child_dag_id = f"resource_planner_task_alloc_webhook_modified_child_{processor_instance}"
deleted_allocation_dag_id = f"resource_planner_task_alloc_webhook_deleted_{processor_instance}"

# Bearer tokens for each event type webhook
webhook_created_bearer_token = f"{company_key}_task_alloc_created_bearer_token_{instance}"
webhook_modified_bearer_token = f"{company_key}_task_alloc_modified_bearer_token_{instance}"
webhook_deleted_bearer_token = f"{company_key}_task_alloc_deleted_bearer_token_{instance}"
