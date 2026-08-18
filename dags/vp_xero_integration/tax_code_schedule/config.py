"""Shared configuration constants for VP -> Xero Tax Code Schedule."""

# pylint: disable=invalid-name

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

integration_name = 'tax_code_schedule'
integration_family = 'vp_xero'
integration_type = 'generic'

main_dag_id_prefix = f'{integration_family}_{integration_name}_main'
main_dag_description = 'Hourly scheduler: triggers Xero tax code sync per enabled customer'
main_dag_tags = ['vantagepoint_xero', integration_name, 'main']

dispatcher_dag_id_prefix = f'{integration_family}_{integration_name}_dispatcher'
dispatcher_dag_description = 'Per-customer dispatcher: triggers processor, gathers errors'
dispatcher_dag_tags = ['vantagepoint_xero', integration_name, 'dispatcher']

processor_dag_id_prefix = f'{integration_family}_{integration_name}_processor'
processor_dag_description = 'Per-customer hourly Xero tax code sync (no populate-once skip gates)'
processor_dag_tags = ['vantagepoint_xero', integration_name, 'processor']

middleware_integration_type = 'tax_code_schedule'

schedule_interval_variable_key_prefix = f'{main_dag_id_prefix}_interval'
# Hourly: Xero tax rates change rarely, but the middleware customer list is
# cheap to poll and a new rate should land in VP within the hour.
default_schedule_interval = '0 * * * *'

middleware_auth_endpoint = '/api/v1/oauth/token'
middleware_integrations_endpoint = '/api/v1/integrations'
middleware_api_base_url_variable_key = 'middleware_api_base_url'

vantagepoint_client_id_variable_key = 'vantagepoint_client_id'
vantagepoint_client_secret_variable_key = 'vantagepoint_client_secret'
