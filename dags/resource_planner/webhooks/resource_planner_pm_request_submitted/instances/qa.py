# pylint: disable=wildcard-import unused-wildcard-import
from resource_planner.webhooks.resource_planner_pm_request_submitted.config import *

instance = "qa"

environment = "pre-production"

company_key = "Repliconpincstream6dev"
replicon_conn_id = "replicon_Repliconpincstream6dev_replicon"

webhook_dag_id = f"resource_planner_pm_request_submitted_webhook_{instance}"
webhook_bearer_token = f"{company_key}_pm_request_submitted_bearer_token_{instance}"

pm_request_processor_dag_id = f"resource_planner_pm_request_processor_{instance}"
