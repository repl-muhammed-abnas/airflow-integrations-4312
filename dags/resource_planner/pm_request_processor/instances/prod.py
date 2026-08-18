# pylint: disable=wildcard-import unused-wildcard-import
from resource_planner.pm_request_processor.config import *

instance = "prod"

environment = "production"

company_key = "RepliconPInc"

replicon_conn_id = "replicon_RepliconPInc_resourceplannertool.integration"

pm_request_processor_dag_id = f"resource_planner_pm_request_processor_{instance}"

resource_planner_pm_request_processor_enable_batch_task = f"resource_planner_pm_request_processor_enable_batch_task_{instance}"
