region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 7
child_dag_max_active_runs = 5
max_active_runs = 1
schedule = '0 * * * *'

# Per-domain sync toggles (default on = current bundled behavior).
# Override per-instance to route domains independently (e.g. direct-cost-only CE->PC).
sync_budget = True
sync_contract = True
sync_direct_cost = True

# Common settings
ce_time_format = '%Y-%m-%dT%H:%M:%SZ'

# Budget sync specific settings - referencing existing integrations
cost_code_segment_type = 'cost_code'
cost_code_segment_name = 'Cost Code'
cost_type_segment_type = 'line_item_type'
cost_type_segment_name = 'Cost Type'

# Cost type settings
revenue_cost_type = 'REVENUE'
revenue_cost_type_name = 'Revenue from the cost code'

# S3 settings for delta processing
aws_conn_id = 'ce_procore_aws_conn'
s3_bucket_name = 'airflow-systemtest'
s3_file_name = 'job_totals_fingerprints.csv'
calculation_strategy = "manual"
budget_uom = 'hours'

# Retry configuration for failed jobs
retry_delays_hours = [0, 3, 6, 12, 24]
retry_buffer_minutes = 5  # Buffer time when checking if retry should happen

# Direct cost sync settings
direct_cost_type = 'expense'
direct_cost_status = 'approved'
internal_email = ['procoreintegrationsupport@deltek.com']
is_paused_upon_creation = True
