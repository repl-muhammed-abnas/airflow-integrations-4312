# pylint: disable=wildcard-import unused-wildcard-import
from resource_planner.webhooks.resource_planner_pm_request_submitted.config import *

instance = "prod"

environment = "production"

company_key = "RepliconPInc"
replicon_conn_id = "replicon_RepliconPInc_resourceplannertool.integration"

webhook_dag_id = f"resource_planner_pm_request_submitted_webhook_{instance}"
webhook_bearer_token = f"{company_key}_pm_request_submitted_bearer_token_{instance}"

pm_request_processor_dag_id = f"resource_planner_pm_request_processor_{instance}"
