region = 'us-east-1'
environment = 'pre-production'

aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'

execution_timeout_days = 7
max_active_runs_webhook_dag = 1
max_active_runs_main_dag = 1
max_active_runs_child_dag = 10
max_active_runs_bulk_dag = 10

event_retention_days = 3
event_clean_interval_hours = 24

schedule_in_seconds = 300
initial_sync_time = '1970-01-01T00:00:00Z'

procore_webhook_fmt = '%Y-%m-%dT%H:%M:%S.%fZ'

rfc_type = 'customer'

allow_zero_amounts = False

IMPORT_FILE_DESCRIPTION_LIMIT = 120
SYNC_CUSTOM_FIELD_LABEL = 'Sync to Computerease'

# Origin ID update settings
# True: write origin_id to Procore only after CE accepts the import (mark ERP sync DAG).
defer_origin_id_until_accepted = False
origin_id_update_schedule_seconds = 300
is_paused_upon_creation = True

# Worklist of change-event links awaiting CE acceptance; child enqueues, mark DAG drains.
origin_id_update_table = {
    'name': 'pending_origin_id_update',
    'columns': ['change_event_id', 'project_id', 'origin_id', 'import_uuid', 'queued_at'],
    'unique_columns': ['change_event_id'],
    'source': []
}

s3_collection = {
    'integration': 'procore_ce_change_orders_sync',
    'tables': [origin_id_update_table]
}
internal_email = ['procoreintegrationsupport@deltek.com']
