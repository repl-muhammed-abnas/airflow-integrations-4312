"""
T-Systems Germany/Iberia Time Off Import Webhook Configuration
Base configuration for webhook DAG that receives time off data from SAP BTP
"""

# Environment settings
environment = 'pre-production'
region = 'eu-central-1'

# DAG execution settings
max_active_runs_webhook = 1 
execution_timeout_days = 14

# Email notifications
internal_logs_email ='{{ var.value.dagrun_internal_testing_email }}'