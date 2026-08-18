"""
Configuration file for Unisys Cost Center Import Integration

This module contains shared configuration values for all DAGs and instances
in the Unisys Cost Center Import integration.

Based on design document: cost_center_design.txt

Configuration Categories:
    - Company Information: Region and environment settings
    - Execution Settings: Timeouts, intervals, and retry configurations
    - Max Active Runs: Concurrent execution limits for each DAG
    - Input File Configuration: CSV header definitions
    - Division Service: Replicon service endpoints and column URIs

Business Logic:
    The integration processes cost center data from input files and syncs with Replicon:
    - Retrieves all existing cost centers (divisions) from Replicon
    - Compares with input data to identify: new cost centers, updates, and disables
    - Creates new cost centers under appropriate company parent
    - Updates cost center names when changed
    - Disables cost centers marked as inactive in input

Design Reference:
    Service Endpoint: /services/DivisionListService1.svc/GetHierarchyData
    Column URIs: full-path, full-path-code, effectively-enabled
    Hierarchy Structure: Company (Level 0) -> Cost Center (Level 1)
"""

# Company Information
region = "us-east-1"
environment = "pre-production"
version = "v1"

# Execution Settings
execution_timeout_days = 14
master_dag_interval = 30
file_sensor_timeout = 10

# Max Active Runs
max_active_run_master = 1
max_active_runs_process_cost_centers = 1
max_active_runs_log_generation = 1

# Parallel Processing
trigger_parallel_dagrun_count_process_cost_centers = 2

# Log file settings
log_file_download_link_expiry_in_sec = 7 * 24 * 60 * 60  # 7 days
gather_cost_center_logs_timeout_hours = 2

# Input File Configuration
# As per design: COMPANY, COMPANY NAME, COST_CENTER, COST_CENTER_NAME, STATUS
input_file_header = "COMPANY,COMPANY_NAME,COST_CENTER,COST_CENTER_NAME,STATUS"

# Division Service Configuration - Based on design document
# Service endpoint for retrieving hierarchy data
division_list_service_endpoint = "/services/DivisionListService1.svc/GetHierarchyData"

# Service endpoint for creating/updating divisions
division_service_endpoint = (
    "/services/DivisionService1.svc/CreateDivisionOrApplyModification"
)
