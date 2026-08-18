region = 'us-east-2'
environment = 'pre-production'

processing_frequency_minutes = 120

post_batch_size = 10000

# OPTIMIZATION: Added configuration for team changes batching
team_changes_batch_size = 10  # Process 10 projects per child DAG instead of 1

extract_report_name = 'Replicon to C1 Team Assignments extract'
report_filter_name = 'UDFFilter_Project4_Taskassignment_billingratechangedate'

debug = False

# OPTIMIZATION: Increased concurrency for better parallel processing
max_webhook_processor_active_dag_runs = 20
max_active_runs = 15
child_dag_max_active_runs = 20

get_webhook_log_name = 'c1_leanstaffassignment_webhooks_v1'

execution_timeout_days = 1

# Export-side throughput optimisation (bulk validation).
# When set to a Variable name (per instance) and enabled, the webhook processor
# logs events on a fast path (no per-event validation/UDF write), and the export
# master validates eligible projects in bulk and writes the tracking UDF only for
# them before running the report. Default None keeps the original per-event
# validation behaviour for every instance. Currently wired on trial only.
export_bulk_validation_var_name = None
