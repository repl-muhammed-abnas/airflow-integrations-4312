region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 7
main_dag_max_active_runs = 1
child_dag_max_active_runs = 5

#sync specific settings
payroll_time_sync_interval_seconds = 300  # How often to run sync

lookback_days = 90
lookahead_days = 30

procore_time_format = '%Y-%m-%d %H:%M:%S'
initial_sync_time = '1970-01-01T00:00:00'

cost_code_segment_type = 'cost_code'
cost_code_segment_name = 'Cost Code'

MAX_CHAR_LENGTH = 60

project_based_on_origin_id = True #If False we check based on procore project number 
employee_based_on_origin_id = True #If False we check basd on procore employee id
project_chunk_size = 100
is_paused_upon_creation = True
pay_types = {
    'Regular Time' : 'regular',
    'Overtime': 'overtime',
    'Double Time': 'double',
    'Sick': 'sick',
    'Vacation': 'vacation',
    'Holiday': 'holiday',
    'Exempt': 'regular',
    'PTO': 'vacation',
    'Salary': 'regular'
}
internal_email = ['procoreintegrationsupport@deltek.com']
