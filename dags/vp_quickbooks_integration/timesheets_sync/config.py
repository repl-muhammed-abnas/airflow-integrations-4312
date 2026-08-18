"""
Shared configuration constants for VP -> QuickBooks Timesheets Sync.
"""
# pylint: disable=invalid-name
region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# First-poll watermark: deliberately recent so onboarding a new tenant
# doesn't auto-backfill all historical timesheets and flood QBO. Ops
# overrides the per-tenant watermark Variable manually for backfills.
# Previously '2025-12-16T03:30:41.203Z' — fetched 5+ months of records
# on first poll, which dragged the dispatcher and burned QBO quota.
initial_sync_time = '2026-05-01T00:00:00.000Z'

billing_transfer_marker = 'Labor Posting - Billing Transfer'

# Default polling cadence for the dispatcher main_dag. Instances can
# override per-tenant via the Airflow Variable
# `vp_qbo_timesheets_sync_schedule_interval_{instance}`.
default_schedule_interval = '*/5 * * * *'

# S3 mapping collection — shared with mapping_sync and vendor_sync.
# employee and firm tables live in the per-customer collection that
# mapping_sync creates and populates; integration_type is hard-pinned
# to 'mapping_sync' so all three integrations hit the same S3 object.
s3_integration_name = 'vp_quickbooks_integration'
s3_mapping_integration_type = 'mapping_sync'
from vp_quickbooks_integration.common.tables import (
    MAP_EMPLOYEE_TABLE_NAME as map_employee_table_name,
    MAP_FIRM_TABLE_NAME as map_firm_table_name,
)
