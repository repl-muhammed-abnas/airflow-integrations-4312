import uuid
import json
import rail
from dxctechnology.compass_iwo_details.utils import custom_methods

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
    }


def get_iwo_wbs_update_reprocess(item):
    return {
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
    return {
        "projects": [
            {
                "uri": null,
                "name": dag_run.conf['parentwbs'] if dag_run.conf['parentwbs'] else dag_run.conf['parentserviceorder'],
                "code": null,
                "parameterCorrelationId": null
            }
        ]
    }


def get_details_for_parentproject_billing_rates_payload():
    return {"projects": [{"uri": rail.result('get_parent_project_details')['uri'], "name": null,
                          "code": null, "parameterCorrelationId": null}]}


def get_update_task_hierarchy():
    data = custom_methods.get_data_from_document(
        rail.result('query_tasks_not_present_in_child_project'))

    def get_user_uris():
        data = custom_methods.get_project_team_members(
            'get_all_project_team_assignment_after_update')
        return list(map(lambda x: {"user": {"uri": x}}, data))if data else []

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
                    "resourcesToAdd": get_user_uris()
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
        "allowTimeEntryAgainstTasksOnly": True
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
