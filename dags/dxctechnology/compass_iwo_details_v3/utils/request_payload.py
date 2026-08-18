import os
import uuid
import json
import rail
from dxctechnology.compass_iwo_details_v3.utils import custom_methods

null = None


def get_task_details(assignments):
    users = custom_methods.get_data_from_document(
        rail.result('get_query_required_users'))

    def get_user_data(empid):
        data = list(
            filter(null, list(filter(lambda user: user['employeeid'] == empid, users))))
        return {"uri": data[0]['uri'], "userstatus": data[0]['userstatus'], "userenddate": data[0]['userenddate']} if data else {}

    def get_json(x):
        values = get_user_data(x['compasspersonnelnumber'])
        return {
            'employeeid': x['compasspersonnelnumber'],
            'assignmentstartdate': x['assignmentstartdate'],
            'assignmentenddate':  x['assignmentenddate'],
            'useruri': values.get('uri'),
            'userstatus': values.get('userstatus'),
            'userenddate': values.get('userenddate')
        }
    return [get_json(x) for x in assignments]


def get_iwo_wbs_update(item):
    return {
        "file_name": os.path.splitext(os.path.split(rail.result('new_file_sensor'))[1])[0],
        "wbs": item['wbs'],
        "parentcompanycode": item['parentcompanycode'],
        "parentcompanycodeuri": rail.result('get_all_object_extension_field')['parentcompanycode'],
        "parentproject": item['parentproject'],
        "parentprojecturi": rail.result('get_all_object_extension_field')['parentproject'],
        "parentwbs": item['parentwbs'],
        "parentwbsuri": rail.result('get_all_object_extension_field')['parentwbs'],
        "parentserviceorder": item['parentserviceorder'],
        "parentserviceorderuri": rail.result('get_all_object_extension_field')['parentserviceorder'],
        "projecttypeuri": rail.result('get_all_object_extension_field')['projecttypeuri'],
        "taskdetails": get_task_details(item['assignments']),
        "iwowbselement": rail.result('get_all_object_extension_field')['iwowbselement'],
        "task_type": rail.result('get_task_type_udf'),
        'gsap_task_required': rail.result('get_all_object_extension_field')['gsap_task_required'],
        "reference_mandatory": rail.result('get_all_object_extension_field')['reference_mandatory'],
        "comments_mandatory": rail.result('get_all_object_extension_field')['comments_mandatory'],
        "psa_flag": rail.result('get_all_object_extension_field')['psa_flag'],
        "iwoindicatoruri": rail.result('get_all_object_extension_field')['iwoindicator']
    }


def get_iwo_wbs_update_reprocess(item):
    return {
        "file_name": item['properties']['dag_conf']['file_name'],
        "wbs":  item['properties']['dag_conf']['wbs'],
        "parentcompanycode":  item['properties']['dag_conf']['parentcompanycode'],
        "parentcompanycodeuri":  item['properties']['dag_conf']['parentcompanycodeuri'],
        "parentproject": item['properties']['dag_conf']['parentproject'],
        "parentprojecturi": item['properties']['dag_conf']['parentprojecturi'],
        "parentwbs": item['properties']['dag_conf']['parentwbs'],
        "parentwbsuri": item['properties']['dag_conf']['parentwbsuri'],
        "parentserviceorder": item['properties']['dag_conf']['parentserviceorder'],
        "parentserviceorderuri": item['properties']['dag_conf']['parentserviceorderuri'],
        "projecttypeuri": item['properties']['dag_conf']['projecttypeuri'],
        "taskdetails": item['properties']['dag_conf']['taskdetails'],
        "iwowbselement": item['properties']['dag_conf']['iwowbselement'],
        "task_type": item['properties']['dag_conf']['task_type'],
        'gsap_task_required': item['properties']['dag_conf']['gsap_task_required'],
        "reference_mandatory": item['properties']['dag_conf']['reference_mandatory'],
        "comments_mandatory": item['properties']['dag_conf']['comments_mandatory'],
        "psa_flag": item['properties']['dag_conf']['psa_flag'],
        "iwoindicatoruri": item['properties']['dag_conf']['iwoindicatoruri']
    }


def get_project_details_payload(dag_run):
    return {
        "projects": [
            {
                "uri": null,
                "name": dag_run.conf['wbs'],
                "code": null,
                "parameterCorrelationId": null
            }
        ]
    }

def get_companycode_details(dag_run):
    return{
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:division-list-column:code",
            "urn:replicon:division-list-column:name"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon:division-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": {
                "uri": null,
                "uris": [],
                "bool": null,
                "date": null,
                "money": null,
                "number": null,
                "text": dag_run.conf['parentcompanycode'],
                "time": null,
                "calendarDayDurationValue": null,
                "workdayDurationValue": null,
                "dateRange": null,
                "dateTimeUtc": null,
                "dateTimeUtcRange": null,
                "numberRange": null
            },
            "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_update_oef_payload():
    return {
        "target": {
            "uri": rail.result('get_child_project_details')['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "objectExtensionFieldsToApply": rail.result('build_all_oef')
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_all_project_team_assignment_payload():
    return {
        "projectUri": rail.result('get_child_project_details')['uri'],
        "asOfDate": null
    }


def get_bulk_update_team_members_payload():
    def get_user_uris():
        return [x['useruri'] for x in rail.result('create_valid_task_list')['valid_tasks'] if x['useruri']
                ] if rail.result('create_valid_task_list') and rail.result('create_valid_task_list')['valid_tasks']else []
    return {
        "projectUri": rail.result('get_child_project_details')['uri'],
        "resourceUri": get_user_uris(),
        "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
    }


def get_project_team_members_assignment_daterange(item):
    return {
        "projectUri": rail.result('get_child_project_details')['uri'],
        "resourceUri": item['useruri'],
        "dateRange": {
            "startDate": item['startdate'],
            "endDate": item['enddate'],
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def get_parent_project_details_payload(dag_run):
    def get_parent():
        if dag_run.conf['parentwbs']:
            return dag_run.conf['parentwbs']

        if dag_run.conf['parentserviceorder']:
            return dag_run.conf['parentserviceorder']

        if dag_run.conf['parentproject']:
            return dag_run.conf['parentproject']

        return null

    return {
        "projects": [
            {
                "uri": null,
                "name": get_parent(),
                "code": null,
                "parameterCorrelationId": null
            }
        ]
    }


def get_details_for_parentproject_billing_rates_payload():
    return {"projects": [{"uri": rail.result('get_parent_project_details')['uri'], "name": null,
                          "code": null, "parameterCorrelationId": null}]}


def get_user_assignment_uris():
    data = custom_methods.get_project_team_members(
        'get_all_project_team_assignment_after_update')
    return list(map(lambda x: {"user": {"uri": x}}, data))if data else []


def get_update_task_hierarchy():
    data = custom_methods.get_data_from_document(
        rail.result('query_tasks_not_present_in_child_project'))

    def get_format_date(entry_date):
        replicon_date = custom_methods.get_replicon_date(
            entry_date)
        return {
            'year': str(replicon_date['year']),
            'month': str(replicon_date['month']),
            'day': str(replicon_date['day'])
        }
    return list(
        map(lambda x: {
            "target": null,
            "parameterCorrelationId": null,
            "taskModificationToApply": {
                "name": x['taskname'],
                "codeToApply": {
                    "value": x['taskcode']
                },
                "descriptionToApply": {
                    "value": x['description']
                },
                "isClosed": x['isclosed'],
                "timeEntryStartDateToApply": {
                    "date": get_format_date(x['startdate'])
                },
                "timeEntryEndDateToApply": {
                    "date": get_format_date(x['enddate'])
                },
                "timeAndExpenseEntryTypeToApply": {
                    "value": x['entrytype']
                },
                "isTimeEntryAllowed": 1,
                "resourceAssignmentModifications": {
                    "resourcesToAdd": get_user_assignment_uris()
                }
            }
        }, data)
    )


def get_add_resource_and_tasks_payload():
    return {
        "project": {
            "uri": rail.result('get_child_project_details')['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "taskHierarchy": get_update_task_hierarchy(),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_allow_timeentry_against_taskonly_payload():
    return {
        "projectUri": rail.result('get_child_project_details')['uri'],
        "allowTimeEntryAgainstTasksOnly": not rail.result('get_parent_project_details')['isTimeEntryAllowed']
    }

def get_update_iwo_wbs_element_payload():
    return {
        "target": {
            "uri": rail.result('get_parent_project_details')['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "nameToApply": null,
            "codeToApply": null,
            "descriptionToApply": null,
            "percentCompletedToApply": null,
            "startDateToApply": null,
            "endDateToApply": null,
            "billingTypeToApply": null,
            "clientBillingAllocationMethodToApply": null,
            "clientAssignmentsSchedulesToApply": null,
            "statusToApply": null,
            "projectWorkflowStateToApply": null,
            "clientRepresentativeToApply": null,
            "programToApply": null,
            "projectLeaderToApply": null,
            "isProjectLeaderApprovalRequired": null,
            "costTypeToApply": null,
            "isTimeEntryAllowed": null,
            "estimatedHoursToApply": null,
            "budgetedHoursToApply": null,
            "estimatedCostToApply": null,
            "budgetedCostToApply": null,
            "expenseBudgetedCostToApply": null,
            "totalEstimatedContractValueToApply": null,
            "defaultBillingCurrencyToApply": null,
            "timeAndMaterials": null,
            "billingContractToApply": null,
            "fixedBid": null,
            "customFieldsToApply": [],
            "resourceAssignmentModifications": null,
            "resourceProjectAssignmentModifications": null,
            "billingContractModifications": null,
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": [
                {
                    "definition": {
                        "uri": rail.result('get_iwo_wbs_element_details')['uri'],
                        "name": null
                    },
                    "tag": null,
                    "numericValue": null,
                    "textValue": rail.result('get_iwo_wbs_element_details')['text'],
                    "fileValue": null,
                    "jsonValue": null
                }
            ]
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_update_billing_rates_payload(item):
    return {
        "projectUri": rail.result('get_child_project_details')['uri'],
        "billingRateUri": item['billingRate']['uri'],
        "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
    }


def get_iwo_blob_update(dag_run):
    return {
        'file_name': dag_run.conf['file_name'],
        'wbs': dag_run.conf['wbs'],
        'parentwbs': dag_run.conf['parentwbs'] if dag_run.conf['parentwbs'] else dag_run.conf['parentserviceorder'],
        'wbsuri': rail.result('get_child_project_details')['uri']
    }


def get_blob_rows(item):
    return [item['wbsUri'], item['wbsName'], item['labourType'], item['labourTypeUri'], item['startDate'], item['endDate']]


def get_json_value_payload(wbs, wbsuri):
    data = custom_methods.get_data_from_document(
        rail.result('write_existing_blob_records'))
    return json.dumps(list(map(lambda item: {
        'wbsUri': wbsuri,
        'wbsName': wbs,
        'labourType': item['labourtype'],
        'labourTypeUri': item['labourtypeuri'],
        'startDate': item['startdate'],
        'endDate': item['enddate']
    }, data)))


def get_update_item_category_payload():
    return {
        "target": {
            "uri": rail.result('get_child_project_details')['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "nameToApply": null,
            "codeToApply": null,
            "descriptionToApply": null,
            "percentCompletedToApply": null,
            "startDateToApply": null,
            "endDateToApply": null,
            "billingTypeToApply": null,
            "clientBillingAllocationMethodToApply": null,
            "clientAssignmentsSchedulesToApply": null,
            "statusToApply": null,
            "projectWorkflowStateToApply": null,
            "clientRepresentativeToApply": null,
            "programToApply": null,
            "projectLeaderToApply": null,
            "isProjectLeaderApprovalRequired": null,
            "costTypeToApply": null,
            "isTimeEntryAllowed": null,
            "estimatedHoursToApply": null,
            "budgetedHoursToApply": null,
            "estimatedCostToApply": null,
            "budgetedCostToApply": null,
            "expenseBudgetedCostToApply": null,
            "totalEstimatedContractValueToApply": null,
            "defaultBillingCurrencyToApply": null,
            "timeAndMaterials": null,
            "billingContractToApply": null,
            "fixedBid": null,
            "customFieldsToApply": [],
            "resourceAssignmentModifications": null,
            "resourceProjectAssignmentModifications": null,
            "billingContractModifications": null,
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": [
                {
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_parent_project_details')[
                            'extensionFieldValues'], 'definition.displayText', 'Item Category', 'definition.uri'),
                        "name": null
                    },
                    "tag": {
                        "uri": null,
                        "slug": null,
                        "tagName": {
                            "name": rail.find_first_by_attr_and_get_attr(rail.result('get_parent_project_details')[
                                'extensionFieldValues'], 'definition.displayText', 'Item Category', 'tag.displayText'),
                            "tagDefinitionUri": null
                        }
                    },
                    "numericValue": null,
                    "textValue": null,
                    "fileValue": null,
                    "jsonValue": null
                }
            ],
            "portfolioToApply": null
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_all_project_tasks_payload(URI):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:task-list-column:task",
            "urn:replicon:task-list-column:full-path",
            "urn:replicon:task-list-column:parent",
            "urn:replicon:task-list-column:enabled",
            "urn:replicon:task-list-column:code",
            "urn:replicon:task-list-column:start-date",
            "urn:replicon:task-list-column:end-date"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:task-list-filter:project"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "uri": URI
                }
            }
        }
    }


def get_put_task_payload(dag_run):
    master_task = rail.result('get_parent_wbs_task_details')[0]

    def get_custom_fields():

        if not master_task['customFields']:
            return []

        dropdown_value = rail.find_first_by_attr_and_get_attr(
            master_task['customFields'], "customField.displayText", 'Task Type', 'text')
        if not dropdown_value:
            return []

        return [
            {
                "customField": {
                    "uri": dag_run.conf['task_type'],
                },
                "dropDownOption": {
                    "name": dropdown_value,
                }
            }
        ]

    def timeEntryDateRange():
        if not dag_run.conf['start_date'] and not dag_run.conf['end_date']:
            return null
        return {
            "startDate": custom_methods.get_replicon_date(dag_run.conf['start_date'], "%d %B %Y"),
            "endDate": custom_methods.get_replicon_date(dag_run.conf['end_date'], "%d %B %Y"),
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }

    return {
        "project": {
            "uri": dag_run.conf['processing_wbs_uri'],
        },
        "task": {
            "target": {
                "name": dag_run.conf['taskname'],
                "parent": {
                    "uri": rail.result('get_parent_task_details')[0]['uri'],
                } if dag_run.conf['level'] != '1' else null,
            },
            "name": dag_run.conf['taskname'],
            "code": null if dag_run.conf['code'] in ['None', None, ''] else dag_run.conf['code'],
            "description": null,
            "percentCompleted": "0",
            "timeEntryDateRange": timeEntryDateRange(),
            "isTimeEntryAllowed": True,
            "isClosed": False,
            "customFieldValues": get_custom_fields(),
            "timeAndExpenseEntryTypeUri": master_task['timeAndExpenseEntryType']['uri']
            if master_task['timeAndExpenseEntryType'] else null,
            "assignedResources": custom_methods.get_resource_uri(dag_run.conf['resources'])
        }
    }

def get_inherit_psa_flag_payload(dag_run):
    def get_parent_psa_flag_tag_uri():
        current_parent_oef_values = rail.result('get_parent_project_details')['extensionFieldValues']
        if current_parent_oef_values:
            psa_flag_parent_tag_uri_parent = rail.find_first_by_attr_and_get_attr(
                current_parent_oef_values, 'definition.displayText', 'PSA Flag', 'tag.uri')
            return psa_flag_parent_tag_uri_parent
        return null

    return {
            "objectUri": rail.result('get_child_project_details')['uri'],
            "value": {
                "definition": {
                    "uri": dag_run.conf['psa_flag'],
                    "name": null
                },
                "tag": {
                    "uri": get_parent_psa_flag_tag_uri(),
                } if get_parent_psa_flag_tag_uri() else null,
                "numericValue": null,
                "textValue": null,
                "fileValue": null,
                "jsonValue": null
            }
        }
