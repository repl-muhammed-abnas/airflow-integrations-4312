region = "eu-central-1"
environment = "pre-production"

# Processing Configuration
process_parallel_count = 2
max_active_runs_master = 1
max_active_runs_child = 4
max_active_runs_log_gen_child = 1
csv_separator = ';'
execution_timeout_days = 14
file_sensor_timeout = 5

# Column Mapping
column_mapping = {
    "Login": "employee_id",
    "FECHA_WORK": "entry_date",
    "HORAS": "hours",
    "PEP": "project_id",
    "TAREA": "full_task_path",
    "Comentarios": "comments",
    "ID": "unique_id"
}

# Validation Configuration
ENTRY_DATE_FORMAT = '%d/%m/%Y'
STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
