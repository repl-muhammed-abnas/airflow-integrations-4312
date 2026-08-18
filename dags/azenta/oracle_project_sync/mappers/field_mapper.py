
PROJECT_FIELDS = ','.join([
    'ProjectId', 'ProjectNumber', 'ProjectName', 'ProjectStatusCode',
    'ProjectStartDate', 'ProjectEndDate', 'ProjectCurrencyCode',
    'ProjectManagerName', 'ProjectManagerEmail',
    'BusinessUnitName', 'BusinessUnitId',
    'ProjectTypeName', 'ProjectDescription',
    'LastUpdateDate',
])

# Fields requested from the projects/{id}/child/Tasks endpoint.
TASK_FIELDS = ','.join([
    'TaskId', 'TaskNumber', 'TaskName', 'TaskLevel',
    'ParentTaskId', 'LowestLevelTask', 'ChargeableFlag',
    'BillableFlag', 'MilestoneFlag',
    'TaskStartDate', 'TaskFinishDate',"TaskPercentComplete","ProgressStatusCode",
])

# Oracle ProjectStatusCode -> Replicon project status.
PROJECT_STATUS_MAP = {
    'ACTIVE': 'In Progress',
    'PENDING_CLOSE': 'In Progress',
    'CLOSED': 'Completed',
}

# Oracle statuses that must never be integrated (logged + excluded for audit).
PROJECT_STATUSES_TO_SKIP = {'DRAFT', 'REJECTED'}

# Only projects carrying this classification are eligible.
REQUIRED_PROJECT_CLASSIFICATION_CATEGORY = 'Project Classification'
REQUIRED_PROJECT_CLASSIFICATION_CODE = 'CUSP - POC'

PROJECT_MANAGER_ROLE_NAME = 'project manager'

# Covers both "Project Manager" and "Project Management Administrator" (same policy).
PM_PERMISSION_POLICY_URN = 'urn:replicon:policy:project-management'

# Managed project key/value (required so the project is created as MANAGED, not unmanaged).
PROJECT_MANAGEMENT_TYPE_KEY_URN = 'urn:replicon:project-key-value-key:project-management-type'
PROJECT_MANAGEMENT_TYPE_URN = 'urn:replicon:project-management-type:managed'

PROJECT_MODIFICATION_SAVE_URN = 'urn:replicon:project-modification-option:save'

# When True, project name becomes "<number> - <name>".
REPLICON_PROJECT_NAME_CONCAT_NUMBER = False

# Generic bucket excluded everywhere (compared lowercase).
GENERIC_LABOR_NAMES = {'labor'}

# Resource names starting with these are excluded everywhere (compared lowercase).
EXCLUDED_RESOURCE_NAME_PREFIXES = ('service commissioning',)

# Field Service Engineering RGs: included in the Resource Groups OEF text, but
# excluded from individual role placeholders (compared lowercase).
FIELD_SERVICE_ENGINEERING_RGS = {
    'field service engineering baf',
    'field service engineering blg',
    'field service engineering che',
    'field service engineering chn',
    'field service engineering jpn',
    'field service engineering man',
    'field service engineering obd',
    'field service engineering sng',
}

# Project-level OEFs written on create only (ADD-only per mapper).
PROJECT_OEF_ADD_ONLY_NAMES = {
    'oracle_project_id': 'Oracle Project Id',
    'oracle_project_type': 'Oracle Project Type',
    'oracle_business_unit_id': 'Oracle Business Unit Id',
    'oracle_project_classification': 'Oracle Project Classification',
}

# Project-level OEFs written on both create and update (ADD-AND-UPDATE per mapper).
PROJECT_OEF_ADD_UPDATE_NAMES = {
    'oracle_business_unit': 'Oracle Business Unit',
}

TASK_OEF_RESOURCE_GROUPS_NAME = 'Resource Groups'

TASK_OEF_ORACLE_TASK_ID_NAME = 'Oracle Task ID'
