region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 1
execution_timeout_days = 1

token_refresh_schedule_interval = '*/30 * * * *'

# ---------------------------------------------------------------------------
# Token Variable name prefix (instance suffix appended at runtime)
# ---------------------------------------------------------------------------
D365_TOKEN_VAR_PREFIX = 'associatedengineeringgroup_d365_access_token'
PIM_TOKEN_VAR_PREFIX = 'associatedengineeringgroup_pim_access_token'

# ---------------------------------------------------------------------------
# Entity Class IDs (PIM internal identifiers)
# ---------------------------------------------------------------------------
ENTITY_CLASS_IDS = {
    'LEAD': 5000,
    'OPPORTUNITY': 7,
    'ENQUIRY': 4,
    'PROJECT': 3,
    'ORGANISATION': 2,
    'CONTACT': 1,
}

# ---------------------------------------------------------------------------
# ExternalIntegrationMapping type names
# ---------------------------------------------------------------------------
MAPPING_TYPE_NAMES = {
    'LEAD': 'D365 JobLead to PIM Lead',
    'OPPORTUNITY': 'D365 Opportunity to PIM Opportunity',
    'ENQUIRY': 'D365 OpportunityProduct to PIM Enquiry',
    'PROJECT': 'D365 ProjectTask to PIM Project',
    'EXTERNAL_ORG': 'D365 Account to PIM Organisation',
    'EXTERNAL_CONTACT': 'D365 Contact to PIM Contact',
    'INTERNAL_CONTACT': 'D365 Employee to PIM Contact',
    'INTERNAL_ORG': 'D365 Company to PIM Internal Organisation',
    'DIVISION': 'D365 Market to PIM Division',
    'OFFICE': 'D365 Office to PIM Office',
    'GROUP': 'D365 Segment to PIM Group',
    'COMPANY': 'D365 Company to PIM Company',
}

# ---------------------------------------------------------------------------
# PIM Custom API paths
# ---------------------------------------------------------------------------
PIM_CUSTOM_API = {
    'LEAD': 'Lead.ashx',
    'OPPORTUNITY': 'Opportunity.ashx',
    'ENQUIRY': 'Enquiry.ashx',
    'PROJECT': 'Project.ashx',
    'DROPDOWN_VALUES': 'DropdownValues.ashx',
    'ENTITY_CONTACTS': 'EntityContacts.ashx',
    'EXTERNAL_INTEGRATION_MAPPING': 'ExternalIntegrationMapping.ashx',
}

# PIM Standard API base path
PIM_STANDARD_API_BASE = '/XWeb/api/v1/'

# D365 to PIM Project Status Mapper
D365_TO_PIM_PROJECT_STATUS = {
    'In Process': 1, # name: Open, reference: InProgress
    'Finished': 7, # name: Closed, reference: Completed
}

# Role ID for linking external orgranization to a project
# This role is used for linkin every external organization to a project
PROJECT_EXTERNAL_ORGANIZATION_ROLE_ID = 156

# D365 API version path
D365_API_VERSION = '/api/data/v9.2/'

# D365 OData annotation suffix for human-readable display values
D365_FORMATTED_VALUE = '@OData.Community.Display.V1.FormattedValue'

# ---------------------------------------------------------------------------
# D365 OData common request headers
# ---------------------------------------------------------------------------
D365_ODATA_HEADERS = {
    'Accept': 'application/json',
    'OData-MaxVersion': '4.0',
    'OData-Version': '4.0',
    'Content-Type': 'application/json; charset=utf-8',
    'Prefer': 'odata.include-annotations="*"',
}

# ---------------------------------------------------------------------------
# Logging / notification
# ---------------------------------------------------------------------------
SUMO_CONN_ID = 'sumologic-dagrunlogger'

tenant_email = ''
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
