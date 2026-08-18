"""
Configuration for T-Systems Project Billing Rate Import integration.

This module defines the base configuration settings for the project billing rate assignment
integration, including file processing, API endpoints, and operational parameters.
"""
# Environment Configuration
region = "eu-central-1"
environment = "pre-production"

time_zone = 'CET'

max_active_runs_api_master = 1

# interval in minutes
master_dag_interval = 60

# Performance Settings
max_active_runs_master = 1
max_active_runs_process_each_payload = 10
max_active_runs_child = 10
max_active_runs_final_logs = 1
execution_timeout_days = 14
file_sensor_timeout_minutes = 10
download_link_validity_days = 7

trigger_parallel_dagrun_count_process_each_payload = 10

gather_child_logs_timeout_hours = 5

lookup_log_timestamp_hours = 24

final_log_generation_dag_schedule_interval = "30 23 * * *"  # Daily at 23:30 UTC

# Billing Rate Name Pattern
# Format: Rate_Type-Billing_Text-Project_ID-CIAM_ID
billing_rate_name_separator = "-"

length_billing_rate_name = 50

default_currency = "EUR"
