"""
Shared configuration constants for VP PSA -> QBO Posted AR Invoice Sync.

Translates Workato recipe 014-503 PSA Poll Vantagepoint Posted AR Invoice
(5-minute polling trigger, skipActivePeriod=true, skipActiveCompany=true).
"""
# pylint: disable=invalid-name
region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# First-poll watermark — deliberately recent so onboarding a new tenant
# doesn't backfill all historical AR invoices. Ops overrides the per-tenant
# watermark Variable manually for backfills.
initial_sync_time = '2026-05-01T00:00:00.000Z'

# Default polling cadence matches the Workato 5-minute trigger.
# Instances can override per-tenant via the Airflow Variable
# `vp_qbo_ar_invoice_sync_schedule_interval_{instance}`.
default_schedule_interval = '*/5 * * * *'
