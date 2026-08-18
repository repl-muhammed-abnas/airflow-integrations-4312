region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 7
max_active_runs = 1
max_active_runs_child = 5
webhook_dag_max_active_runs = 1
schedule_in_seconds = 300

# S3 settings
aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'

# Webhook event format and retention
procore_webhook_fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
event_retention_days = 7
event_clean_interval_hours = 24

# Origin ID update settings
# True: write origin_id to Procore only after CE accepts the import (mark ERP sync DAG).
defer_origin_id_until_accepted = False
origin_id_update_schedule_seconds = 300
is_paused_upon_creation = True

# Worklist of PO links awaiting CE acceptance; child enqueues, mark DAG drains.
origin_id_update_table = {
    'name': 'pending_origin_id_update',
    'columns': ['purchase_order_id', 'project_id', 'origin_id', 'import_uuid', 'queued_at'],
    'unique_columns': ['purchase_order_id'],
    'source': []
}

s3_collection = {
    'integration': 'procore_ce_purchase_order_sync',
    'tables': [origin_id_update_table]
}

# CE Field Lengths (for validation and formatting)
CE_FIELD_LENGTHS = {
    # Summary fields
    'ponum': 10,
    'povennum': 8,
    'poshipto': 30,  # Each of 4 lines
    'poshipvia': 30,
    'pobuyer': 8,
    'posalestaxnum': 8,

    # Line item fields
    'itemdes': 30,  # Each of 10 lines
    'itemqty': 8,
    'itemprice': (6, 2),  # (integer_digits, decimal_digits)
    'itemlocation': 8,
    'itemjob': 6,
    'itemphase': 4,
    'itemcat': 10,
    'itemcosttype': 2,
    'itemequipnum': 8,
    'itemequipcode': 8
}
internal_email = ['procoreintegrationsupport@deltek.com']
