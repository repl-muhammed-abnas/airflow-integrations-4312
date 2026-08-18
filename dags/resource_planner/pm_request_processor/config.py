from pendulum import datetime

region = 'us-east-1'
environment = "pre-production"

master_max_active_run = 5
start_date = datetime(2025, 1, 1)

# Polaris project template the task hierarchy is cloned from when the target
# project has no DPS TCoE tree of its own. Override per-instance if a tenant
# uses a differently-named template.
PROJECT_TEMPLATE_NAME = "2026 - Project Template (DO NOT MODIFY)"

# Anchor task names inside that template's hierarchy.
TASK_HIERARCHY_ROOT_NAME = "DPS TCoE"
REFERENCE_PLACEHOLDER_TASK_NAME = "Custom work - Placeholder 1"

# Airflow Variable key templates (instance-prefixed in instance files)
# resource_planner_pm_request_processor_enable_batch_task = f"resource_planner_pm_request_processor_enable_batch_task_{instance}"
