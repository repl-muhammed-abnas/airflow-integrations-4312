instance = "trial"
region = 'us-east-1'
environment = 'pre-production'

execution_timeout_days = 14
child_dag_max_active_runs = 1

WEBHOOK_ACTION = {
    'UPDATE': 'UPDATE',
    'INSERT': 'INSERT',
    'DELETE': 'DELETE'
}

CHARGE_TYPES = {
    'REGULAR': 'R',
    'OVERHEAD': 'H',
    'PROMOTIONAL': 'P',
}

ROLES = {
    'MANAGER': 'Project Manager',
    'PRINCIPAL':'Project Principal',
    'SUPERVISOR':'Project Supervisor'    
}

ALL_USERS_DEPARTMENT_ID = '1'

PROJECTS_TO_SYNC = ['A', 'I']
# 'ACTIVE': 'A'
# 'INACTIVE': 'I'
# 'DORMANT': 'D'

FILTER_BY_STATUS = True
FILTER_BY_READY_FOR_PROCESSING = False

PROJECT_FIELDS = ','.join([
    'WBSNumber',
    'WBS2',
    'WBS3',
    'Name',
    'StartDate',
    'EndDate',
    'Status',
    'ClientName',
    'ClientID',
    'ClientAddress',
    'ClientCityStateZip',
    'ProjMgr',
    'Principal',
    'Supervisor',
    'ChargeType',
    'ParentId',
    'ReadyForProcessing'
])

WEBHOOK_DATA = {
    'UPDATE': [
        'WBS1',
        'WBS2',
        'WBS3',
        'Name',
        'Action',
        'StartDate',
        'EndDate',
        'Status',
        'OldStatus',
        'ChargeType',
        'ReadyForProcessing',
        'OldReadyForProcessing'
    ],
    'INSERT': [
        'WBS1',
        'WBS2',
        'WBS3',
        'Name',
        'Action',
        'StartDate',
        'EndDate',
        'Status',
        'ChargeType',
        'ReadyForProcessing'
    ],
    'DELETE': [
        'WBS1',
        'WBS2',
        'WBS3',
        'Name',
        'Action',
        'Status',
        'ReadyForProcessing'
    ]
}

timesheet_field_oef_name_for_lc = 'Labor Codes'
enable_budget_labor_codes_level = False
budget_labor_codes_level = "Task" # Task / TimesheetFields
project_resource_enabled = False