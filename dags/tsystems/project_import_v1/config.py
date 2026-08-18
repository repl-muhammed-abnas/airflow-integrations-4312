region = 'eu-central-1'
environment = "pre-production"

# DAG configuration
master_max_active_run = 1
max_active_runs_second_child = 1
max_active_runs_child = 10
execution_timeout_days = 14
parallel_count = 5

schedule_interval = '0 */1 * * *'

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
