# Siemens Portugal - Project Import v1 (GraphQL Implementation)
# Based on Interface Design Document v1.0 - September 2025

region = "eu-central-1"
environment = "pre-production"
execution_timeout_days = 14
max_active_runs_master = 1
max_active_runs_child = 5
time_zone = "Europe/Lisbon"

file_sensor_timeout = 10  # 10 minutes timeout for trial
# Reference File Configuration
reference_file_headers = {
    "Type": "type",
    "Categorization": "categorization",
    "Project Code": "projectcode",
    "Project Manager": "projectmanager",
    "Name": "name",
    "Client": "client",
    "Start date": "startdate",
    "End Date": "enddate",
    "Project Value": "projectvalue",
    "Estimated Engineering hours": "estimatedengineeringhours",
    "Estimated PM hours": "estimatedpmhours",
    "Estimated Engineering cost": "estimatedengineeringcost",
    "Estimated PM cost": "estimatedpmcost",
    "Under Warranty": "underwarranty",
    "Delivery Date": "deliverydate",
}

# Log File Configuration
log_file_headers = ["Project Name", "Project Code", "Status", "Details", "JobID"]

# Change Detection Configuration (REQ-002: Smart Change Detection)
# MD5 hash of all input headers for fingerprinting
fingerprint_algorithm = "md5"
enable_change_detection = True

# Custom Field Names (REQ-009: Custom Field Management)
project_custom_fields = {
    "estimated_engineering_hours": "Estimated Engineering hours",
    "estimated_pm_hours": "Estimated PM Hours",
    "estimated_engineering_cost": "Estimated Engineering cost",
    "estimated_pm_cost": "Estimated PM cost",
    "under_warranty": "Under Warranty",
    "delivery_date": "Delivery Date",
    "project_value": "Project Value",
}

# Default Task List Project (REQ-008: Dynamic Task Management)
default_task_list_project = "Default Task List"

# Budget Calculation Configuration (REQ-007: Budget Calculations)
budget_calculation_fields = {
    "total_hours": ["estimated_engineering_hours", "estimated_pm_hours"],
    "total_cost": ["estimated_engineering_cost", "estimated_pm_cost"],
}
