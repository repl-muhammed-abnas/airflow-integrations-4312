# pylint: disable=wildcard-import unused-wildcard-import
from bearingpoint.webhooks.project_import.config import *

instance = "sandbox"

region = 'eu-central-1'
environment = "pre-production"

company_key = "BearingPointSandbox"

replicon_conn_id = "BearingPointSandbox_repliconint"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

bearingpoint_project_import_bearer_token_variable = f"bearingpoint_project_import_bearer_token_variable_{instance}"

master_dag_id = f"bearingpoint_project_import_master_{instance}"
process_payload_dagid = f'bearingpoint_project_import_process_payload_child_{instance}_v1'
