"""
Shared configuration constants for VP -> QBO Customer Upsert integration.

Source-of-truth direction: Vantagepoint PSA Firm -> QuickBooks Online Customer.
Mirrors `vendor_sync/config.py`; companion module `customer_sync/` syncs the
opposite direction (QBO Customer -> VP Firm) and is unrelated to this one.
"""
# pylint: disable=invalid-name

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

# Backstop for stuck deferred sensors (wait_for_router_dag_runs) so a
# hung child fanout doesn't pin a dispatcher slot forever. Choose long
# enough to drain a normal fanout, short enough to avoid the queue-
# accumulation pattern we observed in E2E (11 queued dispatcher runs).
dispatcher_dagrun_timeout_hours = 2

# First-poll watermark: deliberately recent, NOT a years-ago date.
# A new tenant should NOT auto-backfill a decade of records on first
# run — that floods QBO and burns API quota. To backfill, ops sets the
# per-tenant watermark Variable manually to the desired start.
initial_sync_time = '2026-05-01T00:00:00.000Z'

tenant_email = 'MPTeamReplicon@deltek.com'
