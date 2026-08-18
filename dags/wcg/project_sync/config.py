# WCG Project Sync - Configuration
# Converted from Workato Integration - January 2026
# Original Workato Recipes:
# - live_wcg_netsuite_project_sync_v2_0.recipe.json
# - live_wcg_update_subsidiary_value_on_project.recipe.json
# - live_wcg_update_subsidiary_value_at_system_level.recipe.json

null = None

region = "us-east-1"
environment = "pre-production"
execution_timeout_days = 14
max_active_runs_master = 1
max_active_runs_child = 5
time_zone = "America/New_York"

# File sensor timeout (minutes)
file_sensor_timeout = 10

# Feed File Column Headers Mapping
# Maps CSV headers to internal field names
feed_file_headers = {
    "Internal ID": "internal_id",
    "Name": "name",
    "Customer Internal Id": "customer_internal_id",
    "Customer": "customer",
    "P&L Type": "pl_type",
    "Department (project)": "department",
    "Start Date": "start_date",
    "End Date": "end_date",
    "Total Budget": "total_budget",
    "Project Manager": "project_manager",
    "Project Manager Internal Id": "project_manager_internal_id",
    "Subsidiary": "subsidiary",
    "Status": "status",
    "Replicon Billing Type": "replicon_billing_type",
    "Replicon Cost Type": "replicon_cost_type",
}

# Log File Headers
log_file_headers = [
    "Project Name",
    "Project Code",
    "Customer",
    "Status",
    "Details",
    "JobID",
]

# Project Custom Fields (from Workato integration)
project_custom_fields = {
    "project_subsidiary": "Project Subsidiary",
    "pl_type": "P&L Type",
    "department": "Department",
}

# Project Status Mapping (NetSuite to Replicon)
project_status_mapping = {
    "Active": "urn:replicon:project-status:active",
    "Closed": "urn:replicon:project-status:closed",
    "On Hold": "urn:replicon:project-status:on-hold",
}

# Batch Processing Configuration
batch_polling_max_iterations = 120
batch_polling_interval_seconds = 60

# Batch Task Control Variable (set to 'false' in Airflow Variables to disable batch mode)
can_run_batch_task_var_name = "wcg_project_sync_v2_run_batch_task"

# DAG IDs (overridden by instance configs)
master_dag_id = "wcg_project_sync_v2_master"
process_project_child_dag_id = "wcg_project_sync_v2_process_project_child"
update_subsidiary_dag_id = "wcg_project_sync_v2_update_subsidiary"

# Date format in the feed file
feed_file_date_format = "%m/%d/%Y"

# Currency symbol for budget
default_currency_symbol = "$"

# Project Template Mapper (from lookup_table_data_wcg_project_mapper.csv)
# Maps subsidiary value to template project name in Replicon
# Format: { "subsidiary_name": "template_project_name" }
project_template_mapper = {
    "Analgesic Solutions": "**Analgesic Solutions Project Template**",
    "Applied Clinical Intelligence, LLC": "**Applied Clinical Intelligence Project Template**",
    "CenterWatch": "**CenterWatch Project Template**",
    "Clintrax Global, Inc.": "**Clintrax Global Project Template**",
    "FDAnews": "**FDAnews Project Template**",
    "KMR Group, Inc.": "**KMR Group Inc Project Template**",
    "KMR Group, LLC.": "**KMR Group LLC Project Template**",
    "MedAvante-ProPhase, Inc.": "**MedAvante-ProPhase Inc Project Template**",
    "MedAvante-ProPhase, LLC": "**MedAvante-ProPhase LLC Project Template**",
    "MLIRB": "**MLIRB Project Template**",
    "PatientWise Creative, LLC": "**PatientWise Creative Project Template**",
    "PharmaSeek Financial Services, LLC": "**PharmaSeek Financial Services Project Template**",
    "PharmaSeek, LLC": "**PharmaSeek Project Template**",
    "ProPhase, Inc.": "**ProPhase, Inc. Project Template**",
    "The Avoca Group, LLC": "**The Avoca Group Project Template**",
    "ThreeWire, LLC": "**ThreeWire Project Template**",
    "Trifecta Multimedia, LLC": "**Trifecta Multimedia Project Template**",
    "Velos, LLC": "**Velos Project Template**",
    "VeraSci, LLC": "**VeraSci Project Template**",
    "Vigilare": "**Vigilare Project Template**",
    "WCG Australia Pty Ltd": "**WCG Australia Pty Ltd Project Template**",
    "WCG Clinical, Inc.": "**WCG Clinical Project Template**",
    "WCG Clinical Services Inc.": "**WCG Clinical Services Project Template**",
    "WCG HoldCo IV LLC": "**WCG HoldCo IV LLC Project Template**",
    "WCG Purchaser Corp.": "**WCG Purchaser Corp. Project Template**",
    "WCGIRB": "**WCGIRB Project Template**",
    "WIRB Copernicus Group, Inc": "**WIRB Copernicus Group Project Template**",
    "Intrinsic Imaging, LLC": "**Intrinsic Project Template**",
    "ePS LLC": "**SPAI Project Template**",
}

# Project Copy Options (from Workato CreateProjectCopyBatch2)
project_copy_options = {
    "taskCopyOptionUri": "urn:replicon:project-copy-task-copy-option:copy",
    "teamCopyOptionUri": "urn:replicon:project-copy-team-copy-option:copy",
    "billingRateCopyOptionUri": "urn:replicon:project-copy-billing-rate-copy-option:copy-from-project",
    "expenseCodeCopyOptionUri": "urn:replicon:project-copy-expense-code-copy-option:copy-from-project",
    "taskDateCopyOptionUri": "urn:replicon:task-date-copy-option:shift-by-project-start-date-offset",
}
