# pylint: disable=wildcard-import unused-wildcard-import
from unisys.webhooks.resource_assignment_export.config import *

instance = "qa"

region = 'us-east-1'
environment = "pre-production"

company_key = 'unisysdev'

replicon_conn_id = 'unisysdev_replicon_repliconint'

webhook_master_dag_id = f'unisys_resource_assignment_export_webhook_{instance}'

webhook_log_name = f"unisys_resource_assignment_webhooks_{instance}"
