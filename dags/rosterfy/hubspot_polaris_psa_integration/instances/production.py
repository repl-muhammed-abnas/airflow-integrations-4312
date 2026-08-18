# pylint: disable=wildcard-import unused-wildcard-import
from rosterfy.hubspot_polaris_psa_integration.config import *
region = 'us-east-1'
environment = 'production'
instance = 'prod'
company_key = 'rosterfy'

replicon_conn_id = 'rosterfy_replicon_admin'
sftp_conn_id = 'sftp_useast2'
http_conn_id = f'rosterfy_hubspot_polaris_psa_integration_{instance}_http_conn_id'

deals_endpoint = '/crm/v3/objects/deals/'
companies_endpoint = '/crm/v3/objects/companies/'
contact_endpoint = '/crm/v3/objects/contacts/'
owner_endpoint = '/crm/v3/owners/'
pipeline_endpoint = '/crm/v3/pipelines/deals/'

to_email = "technology@rosterfy.com"
bcc_email ='{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

webhook_renewals_shared_secrete = f"rosterfy_webhooks_renewals_secrete_key_{instance}"
webhook_service_shared_secrete = f"rosterfy_webhooks_service_secrete_key_{instance}"
webhook_sales_shared_secrete = f"rosterfy_webhooks_sales_secrete_key_{instance}"
webhook_update_shared_secrete = f"rosterfy_webhooks_update_project_secrete_key_{instance}"

sales_master_dag = f'rosterfy_hubspot_polaris_psa_integration_sales_master_{instance}'
services_master_dag = f'rosterfy_hubspot_polaris_psa_integration_service_master_{instance}'
renewals_master_dag = f'rosterfy_hubspot_polaris_psa_integration_renewals_master_{instance}'
update_deal_master_dag = f'rosterfy_hubspot_polaris_psa_integration_update_project_master_{instance}'

token_var = f"{company_key}_{instance}_auth_service_token"

can_run_batch_task_var_name = f'rosterfy_hubspot_polaris_psa_integration_{instance}_can_run_batch_task'
lookup_log_timestamp_var = f'rosterfy_hubspot_polaris_psa_integration_lookup_log_timestamp_{instance}'
