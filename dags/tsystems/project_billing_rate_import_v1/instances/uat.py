"""
UAT environment configuration for T-Systems Project Billing Rate Import integration.

This configuration defines the UAT environment settings including connection IDs,
file paths, email addresses, and other environment-specific parameters.
"""

from tsystems.project_billing_rate_import_v1.config import *

# Environment settings
environment = 'pre-production'

# Instance identification
instance = "uat"

# Company and integration identity
company_key = "TSystemsSB"

# Connection IDs for UAT environment
replicon_conn_id = "tsystemssb_replicon_replicon.admin"
sftp_conn_id = "sftp_tsystems_Replicon_Logs"
project_billing_rate_import_http_conn_id = f"tsystems_project_billing_rate_import_http_conn_{instance}"

access_token = f"tsystems_caiman_access_token_variable_{instance}"

sftp_log_filepath = '/TEST/Project Billing Rate Import'

# Email notification settings
tenant_email = "TSI_Replicon@t-systems.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version = "_v1"  # _v1, _v2 etc.
dag_id_suffix = f"{instance}{version}"

# Dynamic DAG ID declarations
api_master_dag_id = f"tsystems_project_billing_rate_assignment_import_webhook_{dag_id_suffix}"  #this was changed to API based setup later by a CR
master_dag_id = f"tsystems_project_billing_rate_import_master_{dag_id_suffix}"
process_each_payload_dag_id = f"tsystems_project_billing_rate_import_process_each_payload_{dag_id_suffix}"
add_billing_rate_dag_id = f"tsystems_project_billing_rate_import_add_billing_rate_child_{dag_id_suffix}"
update_billing_rate_dag_id = f"tsystems_project_billing_rate_import_update_billing_rate_child_{dag_id_suffix}"
add_billing_rate_to_project_and_resource_dag_id = f"tsystems_project_billing_rate_import_add_billing_rate_to_project_and_resource_child_{dag_id_suffix}"
log_generation_dag_id = f"tsystems_project_billing_rate_import_log_generation_child_{dag_id_suffix}"

can_run_batch_task_var_name = f"tsystems_project_billing_rate_import_can_run_batch_task_var_name_{instance}"
