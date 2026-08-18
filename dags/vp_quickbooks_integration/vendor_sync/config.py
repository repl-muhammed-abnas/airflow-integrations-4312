"""
Shared configuration constants for VP QBO Vendor Sync integration.
"""
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

# Fallback VP pay terms when a QBO Term Id isn't in common.tables.PAY_TERMS_MAP.
default_pay_terms = 'Next'

watermark_variable_key_template = watermark_key_template('vendor_sync')
