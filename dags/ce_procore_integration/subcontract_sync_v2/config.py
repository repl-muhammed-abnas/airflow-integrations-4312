region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 7
child_dag_max_active_runs = 5
max_active_runs = 1

# Subcontract sync specific settings
subcontract_sync_interval_minutes = 15
initial_sync_time = '1970-01-01T00:00:00.000Z'
subcontract_accounting_method = 'unit'
calculation_strategy = 'manual'

# Approval status filtering for initial sync
initial_sync_allowed_statuses = ['pending', 'approved']

# Common settings
ce_time_format = '%Y-%m-%dT%H:%M:%S.%fZ'
procore_date_format = '%Y-%m-%d'

# Status mapper from CE approval_status to Procore status
APPROVAL_STATUS_MAPPER = {
    'approved': 'Approved',
    'pending': 'Draft',
    'denied': 'Void'
}


subcontract_per_project_child_dag_id = None
subcontract_vendor_assignment_child_dag_id = None
internal_email = ['procoreintegrationsupport@deltek.com']
is_paused_upon_creation = True
