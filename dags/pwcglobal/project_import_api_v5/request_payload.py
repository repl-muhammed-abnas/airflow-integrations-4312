from datetime import datetime, timedelta
import uuid
import pytz
import rail

from pwcglobal.project_import_api_v5 import custom_method
from pwcglobal.project_import_api_v5.python_callable_method import get_task_state

null = None


billable_charge_code_types_tuple = (
    'External Engagement Project', 'External Project with Credit', 'Internal Project with Credit')
non_billable_charge_code_types_tuple = (
    'Internal Project without Credit', 'Internal Shared Statistical', 'Statistical')


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_today_date_in_paris_timezone():
    now = datetime.now(pytz.timezone('Europe/Paris'))
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_replicon_date(date_str):
    if not date_str:
        return null
    # date format in 2006-04-01
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return null


def get_enabled_division_list():
    return {
        "page": 1,
        "pagesize": 1000,
        "columnUris": [
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:code"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:division-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "bool": True
                }
            }
        }
    }


def get_client_list_payload():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:client-list-column:client",
            "urn:replicon:client-list-column:code",
            "urn:replicon:client-list-column:name"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:client-list-filter:code"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": rail.result('get_client_data')['client_code'],
                }
            }
        }
    }


def get_create_client_payload():
    return {
        "modifications": {
            "nameToApply": {
                "value": rail.result('get_client_data')['client_name']
            },
            "codeToApply": {
                "value": rail.result('get_client_data')['client_code']
            },
            "customFieldsToApply": [{
                "customField": {
                    "uri": rail.find_first_by_attr_and_get_attr(
                        rail.result('get_all_client_custom_fields'), 'displayText', 'Client Party Id', 'uri')
                },
                    "text": rail.result('get_client_data')['client_party_id']
                }] if rail.result('get_client_data')['client_party_id'] else []
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_legal_entity_uri(party_id):
    matched_legal_entity_uris = [x['uri'] for x in rail.result(
        "get_all_legal_entities") if x['fullpath'] == party_id]
    return ",".join(matched_legal_entity_uris) if matched_legal_entity_uris else null


def get_process_project_conf(item, dag_run):

    client_uri = null
    if ((rail.result('get_client_list_from_party_alternate_identifier') and len(
            rail.result('get_client_list_from_party_alternate_identifier')) > 0) and rail.result('get_client_data')['client_code']):
        client_uri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_client_list_from_party_alternate_identifier'), "code", rail.result('get_client_data')['client_code'], "uri")
    if rail.result('create_client_in_replicon'):
        client_uri = rail.result('create_client_in_replicon')['uri']
    if get_task_state('create_client_in_replicon') != 'success':
        if ((rail.result('get_client_list_from_party_alternate_identifier_2') and len(
            rail.result('get_client_list_from_party_alternate_identifier_2')) > 0) and rail.result('get_client_data')['client_code']):
            client_uri = rail.find_first_by_attr_and_get_attr(
                rail.result('get_client_list_from_party_alternate_identifier_2'), "code", rail.result('get_client_data')['client_code'], "uri")

    return {
        **{k.lower(): v for k, v in item.items() if k != 'CurrentStatus'},
        # pylint: disable=unnecessary-comprehension
        **{'internalpersonrole': [dict(x, **{'InternalWorkRelationship': {
            **{key: value for key, value in x['InternalWorkRelationship'].items()},
            'PwCLegalEntity': {
                'PartyId': x.get('InternalWorkRelationship', {}).get('PwCLegalEntity', {}).get('PartyId') if x.get(
                    'InternalWorkRelationship', {}).get('PwCLegalEntity') and x[
                        'InternalWorkRelationship']['PwCLegalEntity'].get('PartyId') else null,
                'pwclegalentityuri': get_legal_entity_uri((x['InternalWorkRelationship']['PwCLegalEntity']['PartyId']).lower())
                if x.get('InternalWorkRelationship', {}).get('PwCLegalEntity') and x[
                    'InternalWorkRelationship']['PwCLegalEntity'].get('PartyId') else null
            }}}) for x in item['InternalPersonRole']] if item.get('InternalPersonRole') else []},
        **{'partyrole': [dict(x, **{'partyiduri': get_legal_entity_uri(x.get('PartyId'))})
                         for x in item['PartyRole']] if item.get('PartyRole') else []},
        **{
            'sender': dag_run.conf['webhook']['data']['Sender'],
            'sender_environment': dag_run.conf['webhook']['data'].get('SenderEnvironment'),
            'identifier': dag_run.conf['webhook']['data'].get('Identifier'),
            'openfortime': (item['CurrentStatus']['OpenForTime']).lower() if item.get('CurrentStatus', {}).get('OpenForTime') else null,
            'confidential_flag': dag_run.conf['webhook']['data']['WorkManagement'][0].get('ConfidentialFlag'),
            'client_uri': client_uri,
            'client_name': rail.result('get_client_data')['client_name'],
            'client_code': rail.result('get_client_data')['client_code'],
            'project_type_oef_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_project_object_extension_field_details"), \
                                                                         "name", "Type", "uri"),
            'project_rate_uri': 'urn:replicon:project-specific-billing-rate',
            'confidential_flag_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_project_custom_fields"), \
                                                                          "displayText", "Confidential Project", "uri"),
            'mandatory_flag_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_project_custom_fields"), \
                                                                       "displayText", "Mandatory Text", "uri"),
            'engagement_manager_party_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_project_custom_fields"), \
                                                                                 "displayText", "Engagement Partner", "uri"),
            'text_effective_date_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_project_custom_fields"), \
                                                                            "displayText", "Text Effective Date", "uri"),
            'project_manager_permission': rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_sets"), \
                                                                               "displayText", "Engagement Manager", "uri"),
            'project_comanager_permission': rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_sets"), \
                                                                                 "displayText", "Engagement Partner", "uri"),
            'replicon_locations': list(map(lambda x: {
                'displayText': x['displayText'], 'uri': x['uri']
            }, rail.result('get_all_locations'))),
            'project_type': rail.find_first_by_attr_and_get_attr(rail.result("get_object_extension_tag_definition_details")['tags'], \
                                                                 "name", item['ChargeCodeType'], "uri") if item.get(
                'ChargeCodeType') else null,
            'confidentiality_flag_dropdown_uri': rail.find_first_by_attr_and_get_attr(
                rail.result("get_confidential_project_custom_field_dropdown_options"), "displayText", "Yes" \
                if dag_run.conf['webhook']['data']['WorkManagement'][0]['ConfidentialFlag'] and
                dag_run.conf['webhook']['data']['WorkManagement'][0]['ConfidentialFlag'] == "true" else "No", "uri"),
            'mandatory_text_dropdown_uri': rail.find_first_by_attr_and_get_attr(
                rail.result("get_mandatory_text_custom_field_dropdown_options"), \
                "displayText", "Yes" if item['MandatoryTextFlag'] and item['MandatoryTextFlag'] == "true" else "No", "uri")
        }
    }


def get_project_uri(chargecode):
    return rail.find_first_by_attr_and_get_attr(
        rail.result('get_project_details_from_code'), 'code', chargecode, 'uri') if rail.result(
            'get_project_details_from_code') else null


def get_add_update_project_conf(item):
    return {
        # pylint: disable=unnecessary-comprehension
        **{k: v for k, v in item.items() if k not in ('_ancestry', '_ecid', '_replication_position')},
        **{
            'project_uri': get_project_uri(item.get('chargecode', '')),
            'load_project': rail.result('load_project'),
            'md5': rail.result('get_md5_from_payload'),
            'log': rail.result('create_log')
        }
    }


def get_department_list_from_cost_center_code(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:code",
            "urn:replicon:department-group-list-column:full-path"
        ],
        "filterExpression":  {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:department-group-list-filter:effectively-enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "bool": True
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:department-group-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf['costcentre']['CostCentreCode']
                    }
                }
            }
        }
    }


def map_to_internal_work_relationship(internal_person_role):
    return [x['InternalWorkRelationship'] for x in internal_person_role if x['InternalWorkRelationship']]


def get_internal_work_relationship_if_valid(internal_person_role):
    return [x for x in map_to_internal_work_relationship(
        internal_person_role) if x.get('InternalPerson') and x['InternalPerson'].get('PartyId') and
        x.get('PwCLegalEntity', {}).get('pwclegalentityuri') and len(
        x['PwCLegalEntity']['pwclegalentityuri'].split(',')) == 1]


def get_user_list_for_team_members(item, dag_run, caller):
    if caller == 'add':
        location_uri = rail.find_first_by_attr_and_get_attr(dag_run.conf['replicon_locations'], 'displayText', rail.result('map_department_list_data')['country'], 'uri')
    else:
        location_uri = dag_run.conf['load_project']['location']['uri'] if dag_run.conf['load_project']['location'] else None
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:division",
            "urn:replicon:user-list-column:location"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": item['InternalPerson']['PartyId']
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "bool": "true"
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:user-list-filter:division"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "value": {
                                "uri": item['PwCLegalEntity']['pwclegalentityuri']
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:user-list-filter:location"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "value": {
                                "uri": location_uri
                            }
                        }
                    }
                }
            }
        }
    } if ((caller == 'add') and location_uri) else  get_user_list_for_managers(item)

def get_user_list_for_managers(item):
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:division"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": item['InternalPerson']['PartyId']
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:division"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "uri": item['PwCLegalEntity']['pwclegalentityuri']
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "bool": "true"
                        }
                    }
                }
            }
        }
    }

def get_time_and_expense_entry_type(charge_code_type):
    if charge_code_type in billable_charge_code_types_tuple:
        return "urn:replicon:time-and-expense-entry-type:billable"
    if charge_code_type in non_billable_charge_code_types_tuple:
        return "urn:replicon:time-and-expense-entry-type:non-billable"
    return "urn:replicon:time-and-expense-entry-type:non-billable"


def get_create_project_payload(dag_run):
    return {
        "modifications": {
            "nameToApply": {
                "value": dag_run.conf['chargecodename']
            },
            "codeToApply": {
                "value": dag_run.conf['chargecode']
            },
            "descriptionToApply": {
                "value": dag_run.conf['engagementline'][0]['EngagementLineDescription']
            } if dag_run.conf.get('engagementline') and dag_run.conf['engagementline'][0].get('EngagementLineDescription') else null,
            "percentCompletedToApply": 0,
            "startDateToApply": {
                "date": get_replicon_date(dag_run.conf['chargecodestartdate'])
            },
            "endDateToApply": {
                "date": get_replicon_date(dag_run.conf['chargecodeenddate'])
            } if dag_run.conf['chargecodeenddate'] else null,
            "billingTypeToApply": {
                "value": "urn:replicon:billing-type:time-and-material"
            },
            "statusToApply": {
                "name": "In Progress" if dag_run.conf['openfortime'] == "true" else
                "Completed"
            },
            "projectLeaderToApply": {
                "user": {
                    "uri": rail.result('get_permissionuri_useruri_project_manager')['user_uri']
                }
            } if rail.result('get_permissionuri_useruri_project_manager') else null,
            "isProjectLeaderApprovalRequired": "true" if rail.result('get_permissionuri_useruri_project_manager')
            else "false",
            "isTimeEntryAllowed": "false" if dag_run.conf.get('workitem') and
            [x['WorkItemTypeId'] for x in dag_run.conf['workitem']]
            else "true",
            "timeAndMaterials": {
                "timeAndExpenseEntryTypeUri": get_time_and_expense_entry_type(dag_run.conf['chargecodetype']),
                "billingRateFrequency": {
                    "name": "Hourly"
                },
                "billingRates": {
                    "billingRate": {
                        "name": "Project Rate"
                    }
                }
            },
            "customFieldsToApply": [
                {
                    "customField": {
                        "uri": dag_run.conf['mandatory_flag_uri']
                    },
                    "dropDownOption": {
                        "uri": dag_run.conf['mandatory_text_dropdown_uri']
                    }
                },
                {
                    "customField": {
                        "uri": dag_run.conf['confidential_flag_uri']
                    },
                    "dropDownOption": {
                        "uri": dag_run.conf['confidentiality_flag_dropdown_uri']
                    }
                }
            ],
            "resourceAssignmentModifications": {
                "resourcesToAdd": [
                    {
                        "departmentGroup": {
                            "uri": rail.result('map_department_list_data')['companycodeuritoassign']
                        }
                    }
                ]
            } if (dag_run.conf['confidential_flag'] == "false") and
            rail.result('map_department_list_data')['accessscopemapper'] and
            rail.result('map_department_list_data')['companycodeuritoassign'] else null,
            "keyValuesToApply": [
                {
                    "keyUri": "urn:replicon:project-key-value-key:project-team-member-assignment-type",
                    "value": {
                        "uri": "urn:replicon:project-team-member-assignment-type:automatically-assign-task",
                    }
                },
                {
                    "keyUri": "urn:replicon:project-key-value-key:source-input-reference-id",
                    "value": {
                        "text": dag_run.conf['md5']
                    }
                }
            ],
            "objectExtensionFieldsToApply": [
                {
                    "definition": {
                        "uri": dag_run.conf['project_type_oef_uri']
                    },
                    "tag": {
                        "uri": dag_run.conf['project_type']
                    }
                }
            ]
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_update_text_effective_udf_value_payload(dag_run):
    return {
        "objectUri": dag_run.conf['project_uri'] if dag_run.conf.get('project_uri') else
        rail.result('get_project_uri'),
        "customFieldUri": dag_run.conf['text_effective_date_uri'],
        "value": get_today_date()
    }


def all_column_setting_payloads(caller):
    user_uri = rail.result(f'get_permissionuri_useruri_{caller}')['user_uri']
    return [
        {
            "userUri": user_uri,
            "listId": "myTeamTimeSheet_list",
            "columnSettings": [
                {
                    "columnUri": "urn:replicon:timesheet-list-column:timesheet",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": False
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 0
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:timesheet-owner",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 220
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:timesheet-status",
                    "settings": [
                        {
                            "key": "addColumnValueToHiddenValues",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 220
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:timesheet-period",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 190
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-info",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 0
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-warning",
                    "settings": [
                        {
                            "key": "width",
                            "value": {
                                "number": 0
                            }
                        },
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-error",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 170
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:total-working-duration",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 100
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:time-off-duration",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 100
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:total-payable-duration",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 100
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:total-count-time-entry-waiting-for-approval-by-approver",
                    "settings": [
                        {
                            "key": "addColumnValueToHiddenValues",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "visible",
                            "value": {
                                "bool": False
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 0
                            }
                        }
                    ]
                }
            ]
        },
        {
            "userUri": user_uri,
            "listId": "timeSheetForApproval_list",
            "columnSettings": [
                {
                    "columnUri": "urn:replicon:timesheet-list-column:timesheet",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": False
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 0
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:timesheet-owner",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 220
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:timesheet-period",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 190
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-info",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 0
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-warning",
                    "settings": [
                        {
                            "key": "width",
                            "value": {
                                "number": 0
                            }
                        },
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-error",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 170
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:total-working-duration",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 100
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:time-off-duration",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 100
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:total-payable-duration",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 100
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:timesheet-status",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": False
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 220
                            }
                        },
                        {
                            "key": "addColumnValueToHiddenValues",
                            "value": {
                                "bool": True
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:timesheet-list-column:total-count-time-entry-waiting-for-approval-by-approver",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": False
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 100
                            }
                        },
                        {
                            "key": "addColumnValueToHiddenValues",
                            "value": {
                                "bool": True
                            }
                        }
                    ]
                }
            ]
        },
        {
            "userUri": user_uri,
            "listId": "project_list",
            "columnSettings": [
                {
                    "columnUri": "urn:replicon:project-list-column:project",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": False
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 0
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:project-list-column:name",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 220
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:project-list-column:status",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 100
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:project-list-column:client",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 220
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:project-list-column:assigned-client-count",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": False
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 0
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:project-list-column:client-effective-date",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": False
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 0
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:project-list-column:client-billing-allocation-method",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": False
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 0
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:project-list-column:start-date",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 110
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:project-list-column:end-date",
                    "settings": [
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        },
                        {
                            "key": "width",
                            "value": {
                                "number": 110
                            }
                        }
                    ]
                },
                {
                    "columnUri": "urn:replicon:project-list-column:total-actual-hours",
                    "settings": [
                        {
                            "key": "width",
                            "value": {
                                "number": 100
                            }
                        },
                        {
                            "key": "visible",
                            "value": {
                                "bool": True
                            }
                        }
                    ]
                }
            ]
        }
    ]


def get_put_policy_data_access_scopes_payload(caller):
    return {
        "userUri": rail.result(f'get_permissionuri_useruri_{caller}')['user_uri'],
        "policyDataAccessScopes": [
            {
                "policyUri": "urn:replicon:policy:project-management",
                "locations": [
                    {
                        "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                        "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:include-descendants"
                    }
                ]
            }
        ]
    }


def get_task_payload(item, dag_run, update_action_type='add'):
    is_triggered_by_update_project = bool(dag_run.conf.get('project_uri'))
    project_uri = dag_run.conf['project_uri'] if is_triggered_by_update_project else rail.result(
        'get_project_uri')
    return {
        "project": {
            "uri": project_uri
        },
        "taskHierarchy": get_payload_by_item_and_dag_run(item, dag_run, is_triggered_by_update_project, update_action_type),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_payload_by_item_and_dag_run(item, dag_run, is_triggered_by_update_project, update_action_type):
    resources_to_add = []

    if is_triggered_by_update_project:
        if update_action_type == 'close':
            return [
                {
                    "target": {
                        "uri": item['uri']
                    },
                    "taskModificationToApply": {
                        "isClosed": 1,
                        "timeEntryEndDateToApply": {
                            "date": get_today_date()
                        }
                    }
                }
            ]

        if update_action_type == 'update':
            end_date_to_apply = null
            end_date_payload = get_replicon_date(
                dag_run.conf['chargecodeenddate'])

            if dag_run.conf['openfortime']:
                if dag_run.conf['openfortime'] == "true":
                    end_date_to_apply = end_date_payload if end_date_payload else null
                elif dag_run.conf['openfortime'] == "false":
                    end_date_to_apply = end_date_payload if end_date_payload else get_today_date()

            return [
                {
                    "target": {
                        "uri": item['uri']
                    },
                    "taskModificationToApply": {
                        "isClosed": 1 if dag_run.conf['openfortime'] == "false" else 0,
                        "timeEntryStartDateToApply": {
                            "date": get_replicon_date(dag_run.conf['chargecodestartdate'])
                        },
                        "timeEntryEndDateToApply": {
                            "date": end_date_to_apply
                        } if end_date_to_apply else null
                    }
                }
            ]

        existing_project_team_members = rail.result(
            'bulk_get_project_team_members')
        if existing_project_team_members:
            user_resource_to_assign = custom_method.get_resource_to_assign_by_type(
                existing_project_team_members, "User", "user")
            if user_resource_to_assign:
                resources_to_add.extend(user_resource_to_assign)

            department_group_resource_to_assign = custom_method.get_resource_to_assign_by_type(
                existing_project_team_members, "Company Code", "departmentGroup")
            if department_group_resource_to_assign:
                resources_to_add.extend(department_group_resource_to_assign)

    return [
        {
            "taskModificationToApply": {
                "name": f"{item['taskname']} - {item['taskcode']}" if is_triggered_by_update_project else
                        f"{item['WorkItemType']} - {item['WorkItemTypeId']}",
                "codeToApply": {
                    "value": item['taskcode'] if is_triggered_by_update_project else
                    item['WorkItemTypeId']
                },
                "isClosed": 0 if dag_run.conf['openfortime'] == "true" else 1,
                "timeEntryStartDateToApply": {
                    "date": get_replicon_date(dag_run.conf['chargecodestartdate'])
                },
                "timeEntryEndDateToApply": {
                    "date": get_replicon_date(dag_run.conf['chargecodeenddate']) if
                    dag_run.conf['chargecodeenddate'] else get_today_date()
                } if is_triggered_by_update_project and dag_run.conf['openfortime'] == "false"
                else null,
                "timeAndExpenseEntryTypeToApply": {
                    "value": get_time_and_expense_entry_type(dag_run.conf['chargecodetype'])
                },
                "isTimeEntryAllowed": 1,
                "resourceAssignmentModifications": {
                    "resourcesToAdd": resources_to_add
                } if resources_to_add else null
            }
        }
    ]


def get_apply_department_resource_to_project_payload():
    return {
        "target": {
            "uri": rail.result('get_project_uri')
        },
        "modifications": {
            "resourceAssignmentModifications": {
                "resourcesToAdd": [
                    {
                        "departmentGroup": {
                            "uri": rail.result('map_department_list_data')['companycodeuritoassign']
                        }
                    }
                ]
            }
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_update_project_team_member_billing_rate_payload(item, dag_run):
    return {
        "projectUri": dag_run.conf['project_uri'] if dag_run.conf['project_uri'] else rail.result(
            'get_project_uri'),
        "resourceUri": item,
        "billingRateUri": dag_run.conf['project_rate_uri'],
        "assigned": "true"
    }


def get_date_range_and_logs(dag_run):
    date_range = null

    logs = {
        'error': [],
        'exception': []
    }

    project_time_entry_date_range = dag_run.conf[
        'load_project']['timeEntryDateRange']
    open_for_time = dag_run.conf['openfortime']
    start_date_to_apply = get_replicon_date(
        dag_run.conf['chargecodestartdate'])
    end_date_to_apply = null
    end_date_payload = get_replicon_date(
        dag_run.conf['chargecodeenddate']) if dag_run.conf['chargecodeenddate'] else null

    if open_for_time:
        if open_for_time == "false":
            date_range = {
                "startDate": project_time_entry_date_range['startDate'],
                "endDate": get_today_date_in_paris_timezone()
            }
            logs['error'].append(
                'Project status updated to completed and end date was updated')
        else:
            if end_date_payload:
                end_date_to_apply = end_date_payload

            if not project_time_entry_date_range['startDate'] or (project_time_entry_date_range['startDate'] and
                                                                  project_time_entry_date_range['startDate'] != start_date_to_apply):

                date_range = get_date_range_add_logs(
                    logs, project_time_entry_date_range, start_date_to_apply, end_date_to_apply, end_date_payload)

            if end_date_payload and not (date_range and date_range.get('startDate')) and (
                not project_time_entry_date_range['endDate'] or (
                    project_time_entry_date_range['endDate'] and project_time_entry_date_range['endDate'] != end_date_payload)):
                date_range = {
                    "startDate": project_time_entry_date_range['startDate']
                }
                if end_date_to_apply:
                    date_range['endDate'] = end_date_to_apply
                    logs['error'].append(
                        'Project end date was updated')
    return {
        'date_range': date_range,
        'project_date_range_logs': logs
    }


def get_datetime_from_replicon_date(replicon_date):
    try:
        date = datetime.strptime(
            f"{replicon_date['year']}-{replicon_date['month']}-{replicon_date['day']}", '%Y-%m-%d')
        return date
    except:  # pylint: disable=bare-except
        return null


def get_datetime_minus_1_day(replicon_date):
    date = get_datetime_from_replicon_date(replicon_date)
    return (date - timedelta(days=1)) if date else null


def get_date_range_add_logs(logs, project_date_range, start_date_to_apply, end_date_to_apply, end_date_payload):
    date_range = null
    if ((get_datetime_from_replicon_date(end_date_payload) > get_datetime_minus_1_day(start_date_to_apply)) if end_date_payload else
            (get_datetime_from_replicon_date(start_date_to_apply) > get_datetime_minus_1_day(start_date_to_apply))):
        date_range = {
            "startDate": start_date_to_apply
        }
        if (project_date_range['startDate'] and get_datetime_from_replicon_date(
                start_date_to_apply) > get_datetime_minus_1_day(project_date_range['startDate'])):
            logs['error'].append(
                'Project start date was updated to a later date')
        if end_date_to_apply:
            date_range['endDate'] = end_date_to_apply
            logs['error'].append(
                'Project end date was updated')
    else:
        logs['exception'].append(
            'Project start date is not updated as its after the end date')

    return date_range


def get_bulk_update_team_members_payload(dag_run):
    return {
        "projectUri": dag_run.conf['project_uri'],
        "userUris": custom_method.get_new_project_team_members(),
        "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
    }
