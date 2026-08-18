"""Shared configuration constants for VP -> QBO Journal Entry Sync."""
# pylint: disable=invalid-name
from vp_quickbooks_integration.common.python_callable_method import (
    watermark_key_template,
)

region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1
initial_sync_time = '2015-12-16T03:30:41.203Z'

tenant_email = 'MPTeamReplicon@deltek.com'

watermark_variable_key_template = watermark_key_template(
    'journal_entry_sync'
)
