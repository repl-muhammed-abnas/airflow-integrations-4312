import uuid
import rail

mandatory_fields = {
    "project_fields": {
        "projectname": "projectname",
        "projectcode": "projectcode"
    }
}

def get_invalid_logs_property_conf(item):
    def get_missing_field():
        not_present_fields = []
        for field in mandatory_fields['project_fields']:
            if item[field] in [None, '']:
                not_present_fields.append(field)
        not_present_fields = list(filter(None, not_present_fields))
        return ";".join(not_present_fields)
    return {
        "projectcode": item['projectcode'],
        "projectname": item['projectname'],
        "clientcode": item['clientcode'],
        "taskcode": item['taskcode'],
        "taskname": item['taskname'],
        'action': 'Add',
        "details": get_missing_field() + " not present in feed file",
        "Status": 'Skipped'
    }

def get_client_data():
    return {
            "page": "1",
            "pagesize": "100",
            "columnUris": [
                "urn:replicon:client-list-column:client"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "filterDefinitionUri": "urn:replicon:client-list-filter:code"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                "value": {
                    "text": rail.result("get_query_data")['clientcode']
                }
                }
            }
        }

def get_service_center_details():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:service-center-list-column:service-center"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "filterDefinitionUri": "urn:replicon:service-center-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
            "value": {
                "text": get_project_data()['companycode']
            }
            }
        }
    }

def does_wbs_exist():
    return bool(rail.result('get_project_details'))

def get_create_project_target_param():
    if does_wbs_exist():
        return {
            "uri": rail.result('get_project_details')['uri']
        }
    return None

def get_project_data():
    return rail.load_all_records(rail.result("get_project_data_from_query"))[0]

def create_projectorapply_modifications(dag_run):
    status= {'1': 'In Progress', '2': 'Completed', '3': 'Cancelled', '4': 'Tentative'}
    def get_client_payload():
        return {
            "clients": [
                {
                "client": {
                    "code": get_project_data()['clientcode'],
                },
                "costAllocationPercentage": "100"
                }
            ],
            "effectiveDate": None
        } if get_project_data()['clientcode'] else None

    modifications = {
        "nameToApply": {
            "value": get_project_data()["projectname"]
        },
        "codeToApply":  {
            "value": get_project_data()["projectcode"]
        } if not does_wbs_exist() else None,
        "descriptionToApply": {
            "value": get_project_data()["projectdescription"]
        } if get_project_data()["projectdescription"] else None,
        "clientAssignmentsSchedulesToApply": get_client_payload(),
        "statusToApply": {
            "name": status[get_project_data()['projectstatus']] if get_project_data()[
                'projectstatus'] and get_project_data()['projectstatus'] in ['1','2','3','4'] else 'In Progress'
        },
        "serviceCenterToApply": {
            "serviceCenter": {
                "uri": rail.result("get_service_center_details")['uri'],
            }
        } if rail.result("get_service_center_details") else None,
        "isTimeEntryAllowed": "0",
        "customFieldsToApply": [
            {
                "customField": {
                    "uri": dag_run.conf['businessarea_customfield'],
                },
                "text": get_project_data()['businessarea']
            }
        ] if get_project_data()['businessarea'] else [],
    }

    return {
        "target": get_create_project_target_param(),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_task_payload(action,data):
    return list(map(lambda task: {
        "target": None if action == "add" else {"uri": task['uri']},
        "taskModificationToApply": {
                "name": task['taskcode'],
                "codeToApply": {
                    "value": task['taskname']
                },
                "isClosed": 0,
                "timeAndExpenseEntryTypeToApply": {
                    "value": "urn:replicon:time-and-expense-entry-type:billable"
                },
                "isTimeEntryAllowed": "1"
            }
    }, data))

def get_update_task_payload():
    return {
        "project": {
            "uri": rail.result('create_project')['uri'],
        },
        "taskHierarchy": get_task_payload("update",rail.result("get_all_task_to_add_update")['tasks_to_update']),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_batch_put_task_payload():
    return {
        "project": {
            "uri": rail.result('create_project')['uri'],
        },
        "taskHierarchy": get_task_payload("add",rail.result("get_all_task_to_add_update")['tasks_to_add']),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_create_client_payload():
    return {
        "client": {
            "target": {
                "code": rail.result('get_query_data')['clientcode']
            },
            "name": rail.result('get_query_data')['clientname'],
            "code": rail.result('get_query_data')['clientcode'],
            "isActive": True
        }
    }

def get_cost_center_uris(dag_run):
    business_area = get_project_data()['businessarea']
    return list(filter(lambda item: item['name'] == business_area, dag_run.conf['cost_centers_data']))

def get_cost_center_payload(dag_run):

    return {
        "projectUri": rail.result('create_project')['uri'],
        "teamMemberDataAccessScopes": [
            {
            "costCenters": [{'uri': item['uri']} for item in get_cost_center_uris(dag_run)],
            }
        ] if get_cost_center_uris(dag_run) else []
    }

def check_groups_data(dag_run):
    message= []
    if not get_cost_center_uris(dag_run) and get_project_data()['businessarea']:
        message.append('business area is not assigned as it is not present in replicon')

    if not bool(rail.result("get_service_center_details")):
        message.append('companycode is not assigned as it is not present in replicon')

    return ','.join(message)
