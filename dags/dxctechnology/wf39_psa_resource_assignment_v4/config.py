region = 'us-east-2'
environment = 'pre-production'

master_dag_interval = 30
file_sensor_timeout = 10

# Parallel processing optimizations
master_dag_active_runs = 1
child_dag_process_wbs_max_active_runs = 10

# Thread pool configuration for bulk operations
thread_pool_size = 1

# Parallel batch processing configuration
parallel_count = 10  # Number of concurrent WBS processing batches

execution_timeout_days = 14

# Log gathering optimization
gather_logs_timeout_hours = 12

extract_report_name = '**C1 Lean staffing Import base report'

# Idempotency / change-detection gate.
# When set to a Variable name (per instance), records already in sync with
# Replicon are skipped to prevent redundant modification webhooks (the root
# cause of duplicate C1 exports). Default None disables the gate for every
# instance except where explicitly overridden. Currently enabled on trial only.
idempotency_gate_var_name = None

