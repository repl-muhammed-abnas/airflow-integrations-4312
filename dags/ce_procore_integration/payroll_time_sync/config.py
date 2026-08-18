region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 7
main_dag_max_active_runs = 1

#sync specific settings
payroll_time_sync_interval_minutes = 30  # How often to run sync

lookback_days = 90
lookahead_days = 30

ce_time_format = '%Y-%m-%dT%H:%M:%S.%f%z'
initial_sync_time = '1970-01-01T00:00:00.000000'

sync_time_entries_as_billable = True

cost_code_segment_type = 'cost_code'
cost_code_segment_name = 'Cost Code'

pay_types = {
    'regular': 'Regular Time',
    'overtime': 'Overtime',
    'double': 'Double Time',
    'sick': 'Sick',
    'vacation': 'Vacation',
    'holiday': 'Holiday',
    'exempt': 'Regular Time',
    'pto': 'Sick',
    'salary': 'Regular Time'
}
internal_email = ['procoreintegrationsupport@deltek.com']
