"""Instance config for VP -> Xero Posted Unit Transaction Sync — dev environment."""
# pylint: disable=invalid-name,unused-import,line-too-long,import-error
from vp_xero_integration.posted_unit_transaction_sync.config import (
    max_active_runs,
    execution_timeout_days,
)

instance = 'dev'
region = 'us-east-1'
environment = 'pre-production'
company_key = f"airflowsandbox{region.replace('-', '')}"
middleware_conn_id = f"middleware_conn_{instance}"

# Recent watermark for dev tenants — avoids backfill of historical
# unit transactions into Xero as manual journals.
initial_sync_time = '2026-05-01T00:00:00.000Z'
