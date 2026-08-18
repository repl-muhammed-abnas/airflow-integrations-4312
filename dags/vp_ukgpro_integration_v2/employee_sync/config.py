"""
Shared configuration constants for VP UKG Pro Employee Sync integration.
"""
# pylint: disable=invalid-name
# STATIC CONFIG - values that are not expected to change per customer/instance
region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1
initial_sync_time = '2025-12-16T03:30:41.203Z'

# REVIEW: Confirm 'MPTeamReplicon@deltek.com' is a monitored service mailbox
# before promoting to production; override per-instance if needed.
tenant_email = 'MPTeamReplicon@deltek.com'

# CONFIG FROM IPA EXTRAS - values that can be set in the per-customer instance file (extras)

