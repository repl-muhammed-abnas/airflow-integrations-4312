region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 7
child_dag_max_active_runs = 5
max_active_runs = 1

# Vendor sync specific settings
vendor_sync_interval_minutes = 10
country_code = 'US'

# Adaptive chunking: below the threshold each vendor gets its own child DAG run
# (preserves per-vendor granularity); above it, vendors are grouped into chunks
# of vendor_sync_chunk_size to collapse PATCH /vendors/sync calls.
vendor_sync_low_volume_threshold = 10
vendor_sync_chunk_size = 1000

# Common settings
ce_time_format = '%Y-%m-%dT%H:%M:%SZ'
internal_email = ['procoreintegrationsupport@deltek.com']
is_paused_upon_creation = True
