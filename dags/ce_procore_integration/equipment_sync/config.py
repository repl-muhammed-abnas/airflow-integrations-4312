region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 7
main_dag_max_active_runs = 1
child_dag_max_active_runs = 5

# Equipment sync specific settings
batch_size = 10  # Number of items to process in each child DAG batch
equipment_sync_interval_minutes = 10
computerease_required_fields = 'uuid,code,description,active'

default_equipment_category = 'Material'
default_equipment_type = 'Item'

# Common settings
ce_time_format = '%Y-%m-%dT%H:%M:%SZ'
initial_sync_time = '1970-01-01T00:00:00Z'
internal_email = ['procoreintegrationsupport@deltek.com']
is_paused_upon_creation = True
