"""Configuration settings for T-Systems Clock In/Out Export integration."""

region = "eu-central-1"
environment = "pre-production"

# Timezone Configuration
timezone = 'Europe/Berlin'  # CET timezone

# Processing Configuration
max_active_runs_master = 1
max_active_runs_child = 5
execution_timeout_days = 14
execution_timeout_mins_write_csv = 30

# Schedule Configuration
schedule_interval = '0 1 * * *'  # Daily at 1:00 AM CET

# Report Configuration
clock_in_out_report_name = 'clock_in_out_report'

# Export Configuration
export_data_type = ['Clock InOut', 'full']  # Export full data, no deltas

# Integration Details
integration_name = 'ClockInOut_REPLICON_OUT'
exporting_system = 'Replicon'
company_code = '6205'

# Employee Type Filter
allowed_employee_types = [
    'Int HR200 Tarif',
    'Int HR200 tariffrei',
    'Int HR200 integr. RZ',
    'Int HR200 FZ=AZ'
]

# Work Type Custom Fields
worktype_fields = [
    'WorkType HR200',
    'WorkType HR200 Tarif', 
    'WorkType HR200 Tariffrei'
]


# Date and Time Formats
date_format_report = '%Y-%m-%d'
date_format_export = '%d.%m.%Y'
time_format_export = '%H:%M'
timestamp_format = '%Y%m%d_%H%M%S'

# File naming
file_prefix = 'ClockInOut_REPLICON_OUT'
