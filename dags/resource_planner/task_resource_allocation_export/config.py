# Shared configuration for resource_planner_task_resource_allocation_export DAG

# DAG configuration defaults
max_active_runs = 1
max_active_runs_child = 4
schedule_interval = None

# Child batching: number of child DAGs to distribute projects across (1-5, ideally 5)
child_batch_count = 5

region = 'us-east-1'
environment = 'pre-production'

# Report configuration
user_report_name = "Resource Planner Project - User Template"
task_report_name = "Resource Planner Project - Task Template"

# GraphQL configuration
graphql_endpoint = '/graphql'
task_batch_size = 50

# Database tables
target_table = 'dbo.dummy_rp_source'
labor_code_table = 'rp_labor_code'

# SFTP configuration for reference snapshot
sftp_conn_id = 'sftp_useast2'
sftp_reference_base_path = '/task_resource_allocation/ref'
sftp_reference_file = 'ref_allocation_data.csv'

# -----------------------------------------------------------------------------
# Failure-notification email (override per-instance)
# -----------------------------------------------------------------------------
# The master DAG sends one email per run when one or more critical tasks
# failed in the master or in any child batch. No DB write.
email_failure_recipients = []  # set per-instance
email_failure_subject_prefix = "[RP Task Resource Allocation Export]"
