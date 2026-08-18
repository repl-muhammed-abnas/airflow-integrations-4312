"""Configuration for Xero -> VP Poll Contact Updates Sync.

Migrates Workato `014-501 PSA Poll Xero Contact updates Vantagepoint`
(Triggers - Polling), which polls Xero every 5 minutes for updated contacts
and syncs new/changed firm records to Deltek VantagePoint.
"""
# pylint: disable=invalid-name
region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# Fallback watermark for tenants that have never run this integration.
# Deliberately recent so onboarding a new tenant does not backfill months of contacts.
initial_sync_time = '2026-07-01T00:00:00.000Z'

# 5-minute poll cadence — matches the Workato ___poll_interval: "5".
# max_active_runs=1 ensures each run completes before the next fires.
default_schedule_interval = '*/5 * * * *'
