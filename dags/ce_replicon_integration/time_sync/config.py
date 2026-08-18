region = 'us-east-1'
environment = 'pre-production'

time_zone = 'US/Eastern'

export_filter_timesheet_status = 'approved'
export_filter_export_status = 'none'

lookback_days = 90
lookahead_days = 30

# DAG execution settings
execution_timeout_days = 14
max_active_runs = 1

# Airflow Connector UI settings
airflow_connector_ui_connid = 'airflow_connector_ui'
hmac_secret = 'airflow_hmac_secret'

TIME_FMT = '%I:%M:%S %p'

paycodes_to_paytypes = {
    'Regular Time' : 'regular',
    'Overtime': 'overtime',
    'Double Time': 'double',
    'Sick': 'sick',
    'Vacation': 'vacation',
    'Holiday': 'holiday'
}
