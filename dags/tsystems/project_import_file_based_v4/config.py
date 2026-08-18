region = 'eu-central-1'
environment = "pre-production"

# DAG configuration
master_max_active_run = 1
max_active_runs_second_child = 1
max_active_runs_child = 10
execution_timeout_days = 14
parallel_count = 5

# Schedule configuration - Check for new files every 30 minutes
schedule_interval_minutes = 30

# File sensor configuration
file_sensor_timeout = 10  # Minutes to wait for new files before timing out

# Log configuration
log_retention_days = 30
sftp_log_path = "/srv/sftpgo/data/Replicon_TARDIS_API/LOGS/Project Master Data Import"

# Status mapping configuration
# Maps SAP project status codes to Replicon project status names
STATUS_MAPPING = {
    "REL": "In Progress",    # Released (active project)
    "FREI": "In Progress",   # Released (active project)
    "TECO": "Completed",     # Technically complete
    "TABG": "Completed",     # Technically complete  
    "CLSD": "Archived",      # Closed
    "ABGS": "Archived"       # Settled/Archived
}

EXTERNAL_EMPLOYEE_TYPES = [
    "External Contractors",
    "External Freelancer",
    "External Manual",
    "External Services",
]
