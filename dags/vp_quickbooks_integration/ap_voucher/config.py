"""Shared configuration constants for VP -> QBO AP Voucher Sync.

Single shared config for both region processor families (US and CA-UK).
The main and dispatcher DAGs are region-agnostic; region is read from the
integration `config.CFG_Region` field at runtime and only selects which
processor a voucher is routed to. Region-specific tax behavior lives in
the processor DAGs / bill-builder helpers, not here.
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

# One watermark per tenant. There is a single dispatcher and each tenant
# belongs to exactly one region (CFG_Region), so the key needs no region
# segment.
watermark_variable_key_template = watermark_key_template('ap_voucher_sync')

# Fallback bill payment period (days) used to compute DueDate when the
# tenant's CFG_DefaultPaymentPeriod is not supplied in the dag_run.conf.
default_payment_period_days = 30

# QBO regional defaults handed to QuickBooksBillOperator (region/currency).
us_qbo_region = 'US'
us_qbo_currency = 'USD'
ca_uk_qbo_region = 'CA'
ca_uk_qbo_currency = 'CAD'
