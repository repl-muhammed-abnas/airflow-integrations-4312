# pylint:disable = too-many-statements
from uuid import uuid4
import rail

null = None
DATE_FORMAT = "%m/%d/%Y"
GROUPS_DELIMITER = '|'

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_value_for_the_key(data, index, pluck_key):
    return data['cells'][index].get(pluck_key)

def get_full_path(full_path_list):
    if not full_path_list:
        return ""
    return GROUPS_DELIMITER.join([item['textValue'] for item in full_path_list])

def filter_full_path_data(response, dag_run):
    if not response['rows']:
        return []

    fullpath_data_list = list(map(lambda data: {
        "name": get_value_for_the_key(data, 0, 'textValue'),
        "uri": get_value_for_the_key(data, 1, 'cellCollection')[-1]['uri'],
        "full_path": get_full_path(data['cells'][1]['cellCollection']),
        "enabled": get_value_for_the_key(data, 2, 'textValue')
    }, response['rows']))

    supervisory_org_list = list(map(lambda d:{
        "uri" : d['uri'],
        "name" : d['name']
        },filter(lambda x:x['full_path']==dag_run.conf['supervisory_org'] and x['enabled'].lower() == 'true', fullpath_data_list)))
    if supervisory_org_list:
        return supervisory_org_list[0]
    return None


MANDATORY_FIELDS = {
    "guid":"guid",
    "permission_name": "permission_name",
    "supervisory_org": "supervisory_org"
}

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")

def get_invalid_record(item):
    details = get_mandatory_fields_exception_message(item)
    return {
        "guid": item['guid'],
        "supervisory_org": item['supervisory_org'],
        "status": "Exception",
        "details": details
    }

def remove_pre_assign_restrictions_payload(dag_run):
    return {
        "target": {
            "loginName": dag_run.conf['guid']
        },
        "modifications": {
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "uri": dag_run.conf['permission_to_be_assigned_uri']
                            },
                            "groupAccessFilter": {
                                "costCenters": [],
                                "locations": []
                            }
                        }
                    ]
                }
            ]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

def get_assigned_supervisory_org_child_levels():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center",
            "urn:replicon:cost-center-list-column:full-path"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:cost-center-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": rail.result('get_supervisory_org_hierarchy_data')['name']
                }
            }
        },
        "hierarchyListDataOptionUris": [
            "urn:replicon:hierarchy-list-data-option:include-descendant-rows"
        ]
    }

def get_supervisory_org_uri_list(assigned_uri_list):
    return list(map(lambda x:{
        "costCenter": {
            "uri": x
        }
    }, assigned_uri_list))

def assign_supervisory_org_payload(dag_run):
    return {
        "target": {
            "loginName": dag_run.conf['guid']
        },
        "modifications": {
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "uri": dag_run.conf['permission_to_be_assigned_uri']
                            },
                            "groupAccessFilter": {
                                "locations": [
                                    {
                                    "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group"
                                    }
                                ],
                                "costCenters": get_supervisory_org_uri_list(rail.result('append_sup_org_to_logged_data')),
                                "divisions": rail.result('get_preassigned_restrictions')['legal_entities_to_be_assigned'],
                                "serviceCenters":rail.result('get_preassigned_restrictions')['classification_to_be_assigned'],
                                "departmentGroups": rail.result('get_preassigned_restrictions')['company_code_to_be_assigned'],
                                "employeeTypeGroups": rail.result('get_preassigned_restrictions')['employeetype_to_be_assigned'],
                            }
                        }
                    ]
                }
            ]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

def get_group_restriction_list(required_restriction_list, group):
    if required_restriction_list:
        return list(map(lambda x:{
            group:{
                "uri":x[group]['uri']
            }
        }, required_restriction_list))
    return []

def get_restrictions_for_TeamManager_for_user(response):
    company_code_to_be_assigned = []
    employeetype_to_be_assigned = []
    legal_entities_to_be_assigned = []
    classification_to_be_assigned = []
    if response:
        company_code_to_be_assigned = get_group_restriction_list(rail.find_first_by_attr_and_get_attr(
            response, 'policyUri', "urn:replicon:policy:team-management", 'departmentGroups'), 'departmentGroup')
        employeetype_to_be_assigned = get_group_restriction_list(rail.find_first_by_attr_and_get_attr(
            response, 'policyUri', "urn:replicon:policy:team-management", 'employeeTypeGroups'), 'employeeTypeGroup')
        legal_entities_to_be_assigned = get_group_restriction_list(rail.find_first_by_attr_and_get_attr(
            response, 'policyUri', "urn:replicon:policy:team-management", 'divisions'), 'division')
        classification_to_be_assigned = get_group_restriction_list(rail.find_first_by_attr_and_get_attr(
            response, 'policyUri', "urn:replicon:policy:team-management", 'serviceCenters'), 'serviceCenter')
        return {
            "company_code_to_be_assigned" : company_code_to_be_assigned,
            "employeetype_to_be_assigned" : employeetype_to_be_assigned,
            "legal_entities_to_be_assigned" : legal_entities_to_be_assigned,
            "classification_to_be_assigned" : classification_to_be_assigned,
        }

def get_current_country(response):
    if response:
        if response['locations']:
            return response['locations'][0]['location']['location']['displayText']
    return None
