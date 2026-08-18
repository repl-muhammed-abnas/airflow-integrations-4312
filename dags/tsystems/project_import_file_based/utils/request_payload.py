"""
Request payload builders for T-Systems Project Import integration
Constructs API request payloads for various Replicon services
"""

import rail
from tsystems.project_import_file_based.utils import custom_methods

# Map SAP billing types to Replicon billing type URIs
billing_type_mapper = {
    'Fixed Bid': "urn:replicon:billing-type:fixed-bid",
    'Time and Material': "urn:replicon:billing-type:time-and-material",
    'Non-Billable': "urn:replicon:billing-type:non-billable"
}

# Map SAP cost types to Replicon cost type URIs
cost_type_mapper = {
    "OpEx": "urn:replicon:cost-type:operational",
    "CapEx": "urn:replicon:cost-type:capital",
    "Unclassified": None  # No mapping for unclassified
}

# Map time/expense entry types for project time tracking configuration
time_expense_entry_mapper = {
    "Billable & Non-Billable": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
    "Billable Only": "urn:replicon:time-and-expense-entry-type:billable",
    "Non-Billable": "urn:replicon:time-and-expense-entry-type:non-billable"
}

def get_client_data(dag_run):
    """Build client search payload for ClientListService"""
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:client-list-column:client",
            "urn:replicon:client-list-column:code",
            "urn:replicon:client-list-column:client-manager",
            "urn:replicon:client-list-column:name"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:client-list-filter:code"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['client_code']
                }
            }
        }
    }

def get_create_client_payload(dag_run):
    
    return {
        "client": {
            "target": {
                "name": f"unknown({dag_run.conf['client_code']})"
            },
            "name": f"unknown({dag_run.conf['client_code']})",
            "code": dag_run.conf['client_code'],
            "isActive": True
        }
    }

def does_wbs_exist():
    """Check if project already exists in Replicon"""
    return bool(rail.result('get_existing_project'))

def get_create_project_target_param():
    """Generate target parameter for project API call"""
    if does_wbs_exist():
        return {
            "uri": rail.result('get_existing_project')['uri']
        }
    return None

def add_oef_entries(dag_run):
    """Build Optional Extension Field entries for project creation/update"""
    oef_list = []

    oef_definition_uri_map = {item['oef_name']: item['uri'] for item in dag_run.conf['project_oef_fields']}
    oef_dropdown_uri_map = {}

    for dropdown in dag_run.conf['project_dropdown_values']:
        for key, values in dropdown.items():
            oef_dropdown_uri_map[key] = {val['name']: val['uri'] for val in values}

    oef_fields_to_check = [
        "project_legal_unit",
        "project_type",
        "control_expert",
        "delivery_cost_center",
        "accounting_group",
        "process_id_group",
        "project_classification"
    ]

    for oef_name in oef_fields_to_check:
        if oef_name in dag_run.conf and dag_run.conf[oef_name]:
            selected_value = dag_run.conf[oef_name]
            definition_uri = oef_definition_uri_map.get(oef_name)
            tag_uri = oef_dropdown_uri_map.get(oef_name, {}).get(selected_value)

            if definition_uri and tag_uri:
                oef_list.append({
                    "definition": {
                        "uri": definition_uri
                    },
                    "tag": {
                        "uri": tag_uri
                    }
                })

    return oef_list

def get_create_or_update_project_payload(dag_run):
    """Build project creation/update payload for Replicon API"""

    billing_type = billing_type_mapper.get(dag_run.conf.get('billing_type', ''),'')
    time_expense_entry = time_expense_entry_mapper.get(dag_run.conf.get('time_expense_entry', ''),'')
    cost_type = cost_type_mapper.get(dag_run.conf.get('cost_type', ''),'')

    modifications = {
        "nameToApply": {
            "value": dag_run.conf['project_name']
        },
        "codeToApply": {
            "value": dag_run.conf['project_code']
        } if not does_wbs_exist() else None,
        "startDateToApply": {
            "date": rail.parse_date(dag_run.conf['start_date'], '%Y-%m-%d')
        } if dag_run.conf.get('start_date') else None,
        "endDateToApply": {
            "date": rail.parse_date(dag_run.conf['end_date'], '%Y-%m-%d')
        } if dag_run.conf.get('end_date') else None,
        "statusToApply": {
            "name": dag_run.conf['status']
        },
        "descriptionToApply": {
            "value": dag_run.conf.get('description', '')
        } if dag_run.conf.get('description') else None,
        "projectLeaderToApply": {
            "user": {
                "employeeId": dag_run.conf['project_manager_id']
            }
        } if dag_run.conf.get('project_manager_id') and rail.result("get_project_manager_details") else None,
        "locationToApply": {
            "location": {
                "uri": dag_run.conf['org_structure_uri'],
            }
        } if dag_run.conf.get('org_structure_uri') else None,
        "departmentGroupToApply": {
            "departmentGroup": {
                "uri": dag_run.conf['cost_center_uri'],
            }
        } if dag_run.conf.get('cost_center_uri') else None,
        "serviceCenterToApply": {
            "serviceCenter": {
                "uri": dag_run.conf['department_group_uri']
            }
        } if dag_run.conf.get('department_group_uri') else None,
        "clientAssignmentsSchedulesToApply": get_client_assignment_payload(dag_run) if dag_run.conf.get('client_code') else None,
        "isTimeEntryAllowed": "0",
        "costTypeToApply": {
            "uri": cost_type
        } if cost_type else None,
        "billingTypeToApply": {
            "value": billing_type
        } if billing_type else None,
        "timeAndMaterials": {
            "timeAndExpenseEntryTypeUri": time_expense_entry,
        } if time_expense_entry and dag_run.conf.get('billing_type', '') != "Non-Billable" else None,
        "objectExtensionFieldsToApply": add_oef_entries(dag_run)
    }

    return {
        "target":  get_create_project_target_param(),
        "modifications": {k: v for k, v in modifications.items() if v is not None},
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": custom_methods.generate_unique_work_id()
    }

def get_client_assignment_payload(dag_run):
    """Build client assignment payload for project billing configuration"""
    return {
        "clients": [
            {
                "client": {
                    "code": dag_run.conf['client_code']
                },
                "costAllocationPercentage": "100"
            }
        ],
        "effectiveDate": None
    }

def get_create_task_payload(dag_run):
    """Build task creation payload for new projects"""
    billing_type_mapper = {
        'Fixed Bid': {
            'uri': 'urn:replicon:time-and-expense-entry-type:billable-and-non-billable',
            'allow_time_entry': True
        },
        'Time and Material': {
            'uri': 'urn:replicon:time-and-expense-entry-type:billable-and-non-billable',
            'allow_time_entry': False
        },
        'Non-Billable': {
            'uri': 'urn:replicon:time-and-expense-entry-type:non-billable',
            'allow_time_entry': True
        },
        'default': {
            'uri': 'urn:replicon:time-and-expense-entry-type:non-billable',
            'allow_time_entry': False
        }
    }

    return {
        "target": None,
        "project": {
            "uri": rail.result('create_or_update_project')['uri']
        },
        "modifications": {
            "name": 'General',
            "codeToApply": {
                "value": dag_run.conf['project_code']
            },
            "timeEntryStartDateToApply": {
                "date": rail.parse_date(dag_run.conf['start_date'], '%Y-%m-%d')
            } if dag_run.conf.get('start_date') else None,
            "timeEntryEndDateToApply": {
                "date": rail.parse_date(dag_run.conf['end_date'], '%Y-%m-%d')
            } if dag_run.conf.get('end_date') else None,
            "timeAndExpenseEntryTypeToApply": {
                "value": billing_type_mapper[dag_run.conf['billing_type']]['uri'] if dag_run.conf['billing_type'] else billing_type_mapper['default']['uri'],
            } if dag_run.conf['billing_type'] != "Non-Billable" else None,
            "isTimeEntryAllowed": "1" if billing_type_mapper.get(dag_run.conf['billing_type'], {}).get('allow_time_entry', False) else "0",
            "isClosed": False
        },
        "unitOfWorkId": custom_methods.generate_unique_work_id()
    }


def put_eligible_project_team_member(dag_run):
    """Build team member assignment payload for project access control"""
    team_deps = dag_run.conf.get('team_departments', {})
    data_access_scopes = []

    assign_from_dept_uris = team_deps.get('assign_from_department_uris', [])
    if assign_from_dept_uris:
        data_access_scopes.append({
            "locations": [],
            "divisions": [],
            "costCenters": [],
            "serviceCenters": [{"uri": uri} for uri in assign_from_dept_uris],
            "employeeTypeGroups": []
        })

    assign_from_employee_type_uris = team_deps.get('assign_from_employee_type_uris', [])
    if assign_from_employee_type_uris:
        data_access_scopes.append({
            "locations": [],
            "divisions": [],
            "costCenters": [],
            "serviceCenters": [],
            "employeeTypeGroups": [{"uri": uri} for uri in assign_from_employee_type_uris]
        })

    return {
        "projectUri": rail.result('create_or_update_project')['uri'],
        "teamMemberDataAccessScopes": data_access_scopes
    }
