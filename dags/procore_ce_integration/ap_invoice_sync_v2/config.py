

region = 'us-east-1'
environment = 'pre-production'

# S3 / AWS settings
aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'

# Webhook event format and retention
procore_webhook_fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
event_retention_days = 7
event_clean_interval_hours = 24

# DAG execution settings
execution_timeout_days = 7
child_dag_max_active_runs = 5
max_active_runs = 1

# Invoice sync specific settings
invoice_fetch_limit = 100
invoice_status_filter = 'approved'  # Only sync approved invoices

# Common settings
ce_time_format = '%Y-%m-%dT%H:%M:%SZ'
procore_time_format = '%Y-%m-%d'

# Default payment terms (in days) for due date calculation
default_payment_terms_days = 30

# Status mapping from Procore to CE
INVOICE_STATUS_MAPPER = {
    'draft': 'DRAFT',
    'approved': 'APPROVED',
    'pending': 'PENDING',
    'paid': 'PAID'
}

# Possible statuses: received, downloaded, accepted, rejected
SKIP_STATUSES = ['accepted', 'downloaded', 'received']

MAX_RETRY_ATTEMPTS = 10
SCHEDULE_INTERVAL_SECONDS = 300

# Origin ID update settings
# True: set requisition origin_id only after CE accepts the import (mark ERP sync
# DAG). False: set eagerly on send (that DAG stays inert). Default False; enabled
# per-instance (qa1/qa3) for testing before flipping the default.
defer_origin_id_until_accepted = False
mark_erp_sync_schedule_seconds = 300
is_paused_upon_creation = True

# Worklist of invoices awaiting CE acceptance; main DAG writes, mark DAG resolves.
origin_id_update_table = {
    'name': 'pending_ap_invoice_origin_id',
    'columns': ['invoice_id', 'invoice_number', 'project_id',
                'commitment_id', 'import_uuid', 'queued_at'],
    'unique_columns': ['invoice_id'],
    'source': []
}

# S3 collection (one db per tenant) created during initial setup.
s3_collection = {
    'integration': 'procore_ce_ap_invoice_sync',
    'tables': [origin_id_update_table]
}

# ComputerEase field validation configuration (based on XSD schema)
CE_FIELD_VALIDATIONS = {
    # Main invoice fields
    'vendor_code': {'display_name': 'Vendor Code', 'char_limit': 8, 'field_type': 'invoice', 'truncate': False},
    'vendor_name': {'display_name': 'Vendor Name', 'char_limit': 30, 'field_type': 'invoice', 'truncate': True},
    'po_number': {'display_name': 'PO Number', 'char_limit': 10, 'field_type': 'invoice', 'truncate': False},
    'invoice_number': {'display_name': 'Invoice Number', 'char_limit': 20, 'field_type': 'invoice', 'truncate': False},
    'description': {'display_name': 'Description', 'char_limit': 30, 'field_type': 'invoice', 'truncate': True},
    'job_code': {'display_name': 'Job Code', 'char_limit': 10, 'field_type': 'invoice', 'truncate': False},

    # Line item fields
    'phase_code': {'display_name': 'Phase Code', 'char_limit': 4, 'field_type': 'line_item', 'truncate': False},
    'category_code': {'display_name': 'Category Code', 'char_limit': 6, 'field_type': 'line_item', 'truncate': False},
    'cost_type': {'display_name': 'Cost Type', 'char_limit': 1, 'field_type': 'line_item', 'truncate': False},
    'item_description': {'display_name': 'Description', 'char_limit': 30, 'field_type': 'line_item', 'truncate': True}
}
internal_email = ['procoreintegrationsupport@deltek.com']
