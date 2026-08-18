region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 7
main_dag_max_active_runs = 1
child_dag_max_active_runs = 5

# Cost Type sync specific settings
cost_type_name = 'Cost Type'
cost_type_type = 'line_item_type'
cost_type_sync_interval_minutes = 60

# Common settings
ce_time_format = '%Y-%m-%dT%H:%M:%SZ'
internal_email = ['procoreintegrationsupport@deltek.com']
is_paused_upon_creation = True
