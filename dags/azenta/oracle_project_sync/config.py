
region = 'us-east-1'
environment = 'pre-production'
# Every 30 minutes 
schedule_interval = '*/30 * * * *'

timezone_iana = 'America/New_York'

execution_timeout_days = 1
max_active_runs_master = 1
max_active_runs_child = 5
max_active_runs_log_gen_child = 1

# Number of per-project child DAG runs to fan out in parallel.
parallel_count = 5

# Presigned download-link validity for the emailed CSV log (7 days).
download_link_expiry_seconds = 7 * 24 * 60 * 60

# Base name for the generated CSV log file (a timestamp is appended).
log_file_name_prefix = 'Azenta_Oracle_Polaris_Sync_Log'

# Page size for Oracle list endpoints (max supported is 100).
ORACLE_PAGE_LIMIT = 100

# Retry settings for Oracle API fetch tasks (retry logic for Oracle 5xx failures).
oracle_api_retries = 3

# ---------------------------------------------------------------------------
# Watermark plumbing
# ---------------------------------------------------------------------------

# Oracle LastUpdateDate watermark format (ISO 8601 with UTC offset to ensure correct timezone interpretation).
WATERMARK_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S+00:00'

# On first run (no stored watermark) look back this many hours.
WATERMARK_INITIAL_LOOKBACK_HOURS = 0.5
