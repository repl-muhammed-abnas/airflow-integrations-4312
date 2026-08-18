"""Shared configuration constants for VP -> Xero Employee Sync Upsert."""

# pylint: disable=invalid-name
from vp_xero_integration.common.python_callable_method import (
    watermark_key_template,
)

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1
initial_sync_time = '2015-12-16T03:30:41.203Z'

integration_name = 'employee_sync_upsert'
integration_family = 'vp_xero'
integration_type = 'generic'

# Airflow connection id for Xero; templated from dag_run.conf.connections.xero
# at task runtime (see processor_dag.py). Fallback matches mapping_sync.
xero_conn_id_default = 'xero_default'

middleware_integration_type = integration_name

main_dag_id_prefix = f'{integration_family}_{integration_name}_main'
dispatcher_dag_id_prefix = f'{integration_family}_{integration_name}_dispatcher'
processor_dag_id_prefix = f'{integration_family}_{integration_name}_processor'

main_dag_description = 'Scheduler DAG for VP -> Xero Employee Sync Upsert'
dispatcher_dag_description = (
    'Poll VP /employee and trigger per-employee map processor'
)
processor_dag_description = (
    'Maintain VP <-> Xero employee map row for one changed employee'
)

# Common tags (per-DAG role tag is appended in each module).
base_tags = ['vantagepoint_xero', integration_name]
main_dag_tags = base_tags + ['main']
dispatcher_dag_tags = base_tags + ['dispatcher']
processor_dag_tags = base_tags + ['processor']

schedule_interval_variable_key_prefix = (
    f'{integration_family}_{integration_name}_schedule_interval'
)
default_schedule_interval = '*/5 * * * *'  # every 5 minutes

middleware_auth_endpoint = '/api/v1/oauth/token'
middleware_integrations_endpoint = '/api/v1/integrations'
middleware_integration_status_filter = 'enabled'
middleware_api_base_url_variable_key = 'middleware_api_base_url'

vantagepoint_client_id_variable_key = 'vantagepoint_client_id'
vantagepoint_client_secret_variable_key = 'vantagepoint_client_secret'
watermark_variable_key_template = watermark_key_template(integration_name)
