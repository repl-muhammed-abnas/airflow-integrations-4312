# pylint: disable=wildcard-import unused-wildcard-import
from resource_planner.webhooks.resource_planner_pm_request_submitted.config import *

instance = "test"

environment = "production"

company_key = "RepliconPIncStream6UAT"
replicon_conn_id = "replicon_RepliconPIncStream6UAT_resourceplannertool.integration"

webhook_dag_id = f"resource_planner_pm_request_submitted_webhook_{instance}"
webhook_bearer_token = f"{company_key}_pm_request_submitted_bearer_token_{instance}"

pm_request_processor_dag_id = f"resource_planner_pm_request_processor_{instance}"
