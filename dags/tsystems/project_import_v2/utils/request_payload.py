"""
Request payload builders for T-Systems Project Import integration
Constructs API request payloads for various Replicon services
"""

import rail
from tsystems.project_import_v2.utils import custom_methods

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
    """
    Build client search payload for ClientListService
    Searches for existing client by code to avoid duplicates
    """
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
        # Filter by exact client code match
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
        "target": None,
        "modifications": {
            "nameToApply": {
            "value": f"unknown({dag_run.conf['client_code']})"
            },
            "codeToApply": {
            "value": dag_run.conf['client_code'],
            },
            "statusToApply": "1",
            "billingRatesToApply": {
            "billingRates": [
                {
                "billingRate": {
                    "uri": "urn:replicon:project-specific-billing-rate"
                }
                },
                {
                "billingRate": {
                    "uri": "urn:replicon:user-specific-billing-rate"
                }
                }
            ]
            }
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": custom_methods.generate_unique_work_id()
        }

def does_wbs_exist():
    """
    Check if project already exists in Replicon
    Used to determine create vs update operation
    """
    return bool(rail.result('get_existing_project'))

def get_create_project_target_param():
    """
    Generate target parameter for project API call
    Returns URI for updates, None for new project creation
    """
    if does_wbs_exist():
        return {
            "uri": rail.result('get_existing_project')['uri']
        }
    return None

def add_oef_entries(dag_run):
    """
    Build Optional Extension Field (OEF) entries for project creation/update
    Maps payload values to Replicon OEF dropdown selections
    """
    oef_list = []

    # Create lookup dictionaries for OEF field definitions and dropdown values
    oef_definition_uri_map = {item['oef_name']: item['uri'] for item in dag_run.conf['project_oef_fields']}
    oef_dropdown_uri_map = {}

    # Build dropdown value lookup map
    for dropdown in dag_run.conf['project_dropdown_values']:
        for key, values in dropdown.items():
            oef_dropdown_uri_map[key] = {val['name']: val['uri'] for val in values}

    # Define OEF fields to process from payload
    oef_fields_to_check = [
        "project_legal_unit",      # Legal entity information
        "project_type",           # Project categorization
        "control_expert",         # Control/audit designation
        "delivery_cost_center",   # Delivery organization
        "accounting_group",       # Financial grouping
        "process_id_group",       # Process identification
        "project_classification"  # Contract type classification
    ]

    # Process each OEF field if present in payload
    for oef_name in oef_fields_to_check:
        if oef_name in dag_run.conf and dag_run.conf[oef_name]:
            selected_value = dag_run.conf[oef_name]

            # Get OEF field definition URI
            definition_uri = oef_definition_uri_map.get(oef_name)

            # Get dropdown option URI for the selected value
            tag_uri = oef_dropdown_uri_map.get(oef_name, {}).get(selected_value)

            # Only add OEF entry if both definition and value URIs are found
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
    """
    Build comprehensive project creation/update payload for Replicon API
    Handles both new project creation and existing project modifications
    """

    # Map payload values to Replicon URIs using the defined mappers
    billing_type = billing_type_mapper.get(dag_run.conf.get('billing_type', ''),'')
    time_expense_entry = time_expense_entry_mapper.get(dag_run.conf.get('time_expense_entry', ''),'')
    cost_type = cost_type_mapper.get(dag_run.conf.get('cost_type', ''),'')

    # Build project modifications payload - only include fields with values
    modifications = {
        # Core project information
        "nameToApply": {
            "value": dag_run.conf['project_name']
        },
        # Project code only set for new projects (not updates)
        "codeToApply": {
            "value": dag_run.conf['project_code']
        } if not does_wbs_exist() else None,

        # Project timeline dates
        "startDateToApply": {
            "date": rail.parse_date(dag_run.conf['start_date'], '%Y-%m-%d')
        } if dag_run.conf.get('start_date') else None,
        "endDateToApply": {
            "date": rail.parse_date(dag_run.conf['end_date'], '%Y-%m-%d')
        } if dag_run.conf.get('end_date') else None,
        
        # Project status mapping from SAP to Replicon
        "statusToApply": {
            "name": dag_run.conf['status']
        },

        # Optional project description
        "descriptionToApply": {
            "value": dag_run.conf.get('description', '')
        } if dag_run.conf.get('description') else None,

        # Project manager assignment (only if user exists in Replicon)
        "projectLeaderToApply": {
            "user": {
                "employeeId": dag_run.conf['project_manager_id']
            }
        } if dag_run.conf.get('project_manager_id') and rail.result("get_project_manager_details") else None,

        # Organizational structure assignments
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

        # Client assignment for billing purposes
        "clientAssignmentsSchedulesToApply": get_client_assignment_payload(dag_run) if dag_run.conf.get('client_code') else None,

        # Time entry configuration - always enabled per business requirements
        "isTimeEntryAllowed": "0",

        # Financial and billing configurations
        "costTypeToApply": {
            "uri": cost_type
        } if cost_type else None,
        "billingTypeToApply": {
            "value": billing_type
        } if billing_type else None,
        "timeAndMaterials": {
            "timeAndExpenseEntryTypeUri": time_expense_entry if time_expense_entry else None,
            "billingRates": [
                {
                    "billingRate": {
                        "uri": "urn:replicon:project-specific-billing-rate",
                    }
                },
                {
                    "billingRate": {
                        "uri": "urn:replicon:user-specific-billing-rate",
                    }
                }
            ]
        } if dag_run.conf.get('billing_type', '') not in ("Non-Billable", "Fixed Bid") and not does_wbs_exist() else None,

        # Optional Extension Fields for additional project metadata
        "objectExtensionFieldsToApply": add_oef_entries(dag_run)
    }

    return {
        "target":  get_create_project_target_param(),  # URI for updates, None for creates
        "modifications": {k: v for k, v in modifications.items() if v is not None},  # Filter out None values
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": custom_methods.generate_unique_work_id()  # Unique identifier for this operation
    }

def get_client_assignment_payload(dag_run):
    """
    Build client assignment payload for project billing configuration
    Assigns 100% cost allocation to the specified client
    """
    return {
        "clients": [
            {
                "client": {
                    "code": dag_run.conf['client_code']
                },
                "costAllocationPercentage": "100"  # Full allocation to this client
            }
        ],
        "effectiveDate": None  # Effective immediately
    }

def get_create_task_payload(dag_run):
    """
    Build task creation payload for new projects
    Creates "General" task with billing type-specific configuration
    """
    # Map billing types to task-specific time entry configurations
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
        "target": None,  # New task creation
        "project": {
            "uri": rail.result('create_or_update_project')['uri']  # Link to parent project
        },
        "modifications": {
            "name": 'General',  # Standard task name for all projects
            "codeToApply": {
                "value": dag_run.conf['project_code']  # Task code matches project code
            },
            # Task time entry window matches project timeline
            "timeEntryStartDateToApply": {
                "date": rail.parse_date(dag_run.conf['start_date'], '%Y-%m-%d')
            } if dag_run.conf.get('start_date') else None,
            "timeEntryEndDateToApply": {
                "date": rail.parse_date(dag_run.conf['end_date'], '%Y-%m-%d')
            } if dag_run.conf.get('end_date') else None,

            # Configure time tracking based on billing type
            "timeAndExpenseEntryTypeToApply": {
                "value": billing_type_mapper[dag_run.conf['billing_type']]['uri'] if dag_run.conf['billing_type'] else billing_type_mapper['default']['uri'],
            } if dag_run.conf['billing_type'] != "Non-Billable" else None,
            "isTimeEntryAllowed": "1" if billing_type_mapper.get(dag_run.conf['billing_type'], {}).get('allow_time_entry', False) else "0",
            "isClosed": False  # Task starts open for time entry
        },
        "unitOfWorkId": custom_methods.generate_unique_work_id()
    }


def put_eligible_project_team_member(dag_run):
    """
    Build team member assignment payload for project access control
    Version 1.3: Enhanced with assignment restrictions (departments and employee types)
    Grants project access to users in specified service centers

    Creates separate data access scope objects:
    1. If departments present: scope with serviceCenters + assignTeamFromServiceCenters
    2. If employee types present: scope with employeeTypeGroups
    """
    team_deps = dag_run.conf.get('team_departments', {})
    data_access_scopes = []

    assign_from_dept_uris = team_deps.get('assign_from_department_uris', [])
    # Add service centers scope if departments are present
    if assign_from_dept_uris:
        data_access_scopes.append({
            "locations": [],
            "divisions": [],
            "costCenters": [],
            "serviceCenters": [{"uri": uri} for uri in assign_from_dept_uris],
            "employeeTypeGroups": []
        })

    # Add employee type scope if employee types are present
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
