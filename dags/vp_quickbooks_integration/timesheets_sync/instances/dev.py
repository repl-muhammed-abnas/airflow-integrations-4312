"""Instance configuration for VP -> QBO Timesheets Sync — dev environment."""
# pylint: disable=invalid-name,unused-import,line-too-long,import-error
from vp_quickbooks_integration.timesheets_sync.config import (
    max_active_runs,
    execution_timeout_days,
)

instance = 'dev'
region = 'us-east-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"

# Dev tenants started exercising this DAG in May 2026; clamp the
# initial poll window so onboarding a fresh dev tenant doesn't drag in
# unrelated history from the global default in config.py.
initial_sync_time = '2026-05-01T00:00:00.000Z'
