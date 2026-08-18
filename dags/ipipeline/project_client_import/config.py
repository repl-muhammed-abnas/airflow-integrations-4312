# Salesforce to Replicon Integration Configuration
# iPipeline Project and Client Import

# AWS Configuration
region = 'us-east-1'
environment = 'pre-production'

# Timezone Configuration
time_zone = "America/New_York"

# can be implemented in later stages
# lookback_minutes_for_query = 15  # Hours to look back for updated/new records

# DAG Execution Settings
execution_timeout_days = 14
gather_logs_timeout_hours = 2
gather_errors_from_child_timeout_hours = 2

max_active_run_master = 1
child_dag_max_active_runs = 5

parallel_count_process_clients = 5
parallel_count_process_projects = 5

# Sync Configuration
master_dag_interval = 30  # in minutes

TIMESTAMP_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
 
# Salesforce Query Settings
ACCOUNT_TYPES_TO_SYNC = ('Customer',)  # Tuple for potential future expansion (Drop down value, no need for lowercase comparison)
COMPANY_DBA_NAMES_TO_EXCLUDE_IN_SYNC = ('individual', 'Individual', 'INDIVIDUAL')
OPPORTUNITY_STAGES_TO_EXCLUDE = ["High Level Estimate"]

# Engagement Type Configuration
PRODUCTIVE_ENGAGEMENT_TYPES = [
    'Billable - Fixed Price',
    'Billable - Time and Materials',
    'BlueSun - Billable - Fixed Fee',
    'BlueSun - Billable - Time and Materials',
    'Infrastructure Projects',
    'Investment Projects',
    'IPL - Billable - Fixed Fee',
    'Network Operations',
    'Product Feature/Process Improvement',
    'Product Upgrades',
    'Research & Development',
    'Support & Maintenance (Subscription)',
    'TCP - Additional Services',
    'TCP - Billable - Fixed Fee',
    'TCP - Billable - Time and Materials',
    'TCP - Governance',
    'TCP - Standard Production Support',
    'UK - IPL - Non-Billable - Project-Support'
]

PROD_SUPPORT_ENGAGEMENT_TYPES = ['Production Support']

# Stage to Replicon Status Mapping
PROJECT_STATUS_MAP = {
    'Proposal': {'name': 'In Progress'},
    'Negotiation': {'name': 'In Progress'},
    'Contracting': {'name': 'In Progress'},
    'Stage 6 – Closed Won': {'name': 'In Progress'},
    'Stage 9 – Closed Lost': {'name': 'Completed'},
    'Closed Won – Services Completed': {'name': 'Completed'}
}

PROJECT_BILLING_MAP = {
    'Time & Materials': {'value': 'urn:replicon:billing-type:time-and-material'},
    'Fixed Fee': {'value': 'urn:replicon:billing-type:fixed-bid'},
    'Fixed Fee & Expenses': {'value': 'urn:replicon:billing-type:fixed-bid'}
}

MANDATORY_FIELDS_NEW_CLIENT = ['Name', 'Id']
MANDATORY_FIELDS_NEW_PROJECT = [
    'Name', 'Id', 'Project_Start_Date__c', 'StageName', 'AccountId', 'Project_Code__c']

CURRENCIES_MAP = {
    "USD": "US Dollar",
    "CAD": "Canadian Dollar",
    "GBP": "British Pound"
}

COPY_TEMPLATE_PROJECT_DETAILS = {
    'name': "_Template_Billable",
    'code': "TB"  # Since code is the unique identifier, we are using this value
}

REVENUE_CONTRACT_POLICIES_ALLOWED_FOR_UPDATE = ['Percentage of Completion', 'Draw Down', 'Equal Distribution (Straight Line)']
