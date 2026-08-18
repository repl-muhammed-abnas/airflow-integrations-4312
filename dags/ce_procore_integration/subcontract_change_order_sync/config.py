region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 7
main_dag_max_active_runs = 1
child_dag_max_active_runs = 5
line_item_sync_dag_max_active_runs = 5

# Purchase order sync specific settings
subcontract_change_order_sync_interval_minutes = 30  # How often to run sync

# Common settings
time_format = '%Y-%m-%dT%H:%M:%SZ'
internal_email = ['procoreintegrationsupport@deltek.com']
is_paused_upon_creation = True
