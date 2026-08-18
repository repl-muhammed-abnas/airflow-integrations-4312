# pylint: disable=invalid-name

region = 'all'
environment = 'all'

middleware_conn_id = 'middleware_conn_id'
maintenance_api_endpoint = '/api/v1/admin/maintenance/cleanup'

max_active_runs = 1
execution_timeout_hours = 3
retries = 3
retry_delay_minutes = 5

max_db_entry_age_var_name = 'airflow_db_cleanup__max_db_entry_age_in_days'
chunk_size_var_name = 'airflow_db_cleanup_chunk_size'
admin_token_var_name = 'airflow_db_cleanup__admin_token'

default_max_db_entry_age_days = 60
default_chunk_size = 5000
