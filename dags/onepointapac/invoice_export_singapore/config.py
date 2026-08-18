region = 'eu-central-1'
environment = 'qa'
execution_timeout_days = 14
max_active_runs = 1
max_active_runs_invoice_export_child = 5

# Number of parallel lanes trigger_parallel_dagrun fans invoice child triggers across.
parallel_count_invoice_export = 3

master_schedule_interval = 5

# Recipe param "CreateContact" = "Yes": create the Xero contact when the client is missing.
createNewContactXero = True

# The recipe only processes Singapore invoices; currency is read from the invoice list
# cell text (e.g. "SGD$1,234.00").
REQUIRED_CURRENCY_PREFIX = 'SGD$'
CURRENCY_CODE = 'SGD'

# The recipe stops (no error) for invoices already Billed or Paid.
SKIP_INVOICE_STATUSES = ('Billed', 'Paid')

# Xero invoice Reference is "Proforma Invoice #<replicon invoice number>".
REFERENCE_PREFIX = 'Proforma Invoice #'

# Replicon invoice-item metadata key holding the ad-hoc line description.
INVOICE_ITEM_DESCRIPTION_KEY = 'urn:replicon:invoice-item-metadata-key:description'

# Replicon extension-field names written back after a successful sync.
SYNC_STATUS_FIELD_NAME = 'Sync Status'
SYNC_STATUS_SYNCED_TAG = 'Synced'
EXTERNAL_SYSTEM_INVOICE_FIELD_NAME = 'External System Invoice #'
SYNC_NOTE_FIELD_NAME = 'Sync Note'
