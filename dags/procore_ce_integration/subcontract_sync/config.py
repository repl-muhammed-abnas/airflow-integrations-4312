region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 1
max_active_runs = 1
max_active_runs_child = 5

schedule_in_seconds = 60

# S3 settings
aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'

# Webhook settings
procore_webhook_fmt = '%Y-%m-%dT%H:%M:%S.%fZ'

# Event retention settings
event_retention_days = 7
event_clean_interval_hours = 24
max_active_runs_attachment_child = 10

# Integration settings
integration_type = 'generic'

download_link_expiry_seconds = 7 * 24 * 60 * 60
json_filename = 'subcontract.json'
subcontract_format = 'CONTRACT'

ce_import_type = 'subcontract'
resource_work_order_contract = 'Work Order Contracts'
syncable_event_types = ['create', 'update']
syncable_subcontract_statuses = ['approved']
approval_status_mapper = {
    'Approved': 'approved',
    'Draft': 'pending',
    'Void': 'denied'
}

# Import polling settings
import_poll_timeout_minutes = 2
import_poll_interval_seconds = 10

# If True, line items with amount == 0 will be excluded from the CE payload.
skip_zero_amount_line_items = True
is_paused_upon_creation = True

MAX_CHAR_LEN_DESCRIPTION = 60
internal_email = ['procoreintegrationsupport@deltek.com']
