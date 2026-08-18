"""Global configuration for the MMR Consulting invoice export."""

region = 'us-east-1'
environment = 'pre-production'
execution_timeout_days = 14
child_dag_max_active_runs = 10
max_active_runs_invoice_export_child = 5

# Global settings
DEFAULT_ACCOUNT_CODE = '4010'
createNewContactXero = True
TRACKING_OPTION = 'BC'
REQUIRED_INVOICE_STATUS = 'Invoiced'
PO_TYPE_FIELD_NAME = 'PO Type'

# Metadata keys
INVOICE_PO_NUMBER_KEY = 'urn:replicon:invoice-metadata-key:po-number'
INVOICE_ITEM_DESCRIPTION_KEY = 'urn:replicon:invoice-item-metadata-key:description'
