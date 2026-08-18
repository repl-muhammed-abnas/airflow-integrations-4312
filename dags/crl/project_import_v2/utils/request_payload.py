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
                "urn:replicon:client-list-column:client",
                "urn:replicon:client-list-column:code"
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

def get_service_center_details_for_project():
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

def get_service_center_details_for_task(item):
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
                "text": item['businessarea']
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
        } if not does_wbs_exist() else None,
        "codeToApply":  {
            "value": get_project_data()["projectcode"]
        },
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
                "uri": rail.result("get_service_center_details_for_project")['uri'],
            }
        } if rail.result("get_service_center_details_for_project") else None,
        "isTimeEntryAllowed": "0",
        "customFieldsToApply": [
            {
                "customField": {
                    "uri": dag_run.conf['project_custom_fields']
                },
                "text": get_project_data()['project_business_area']
            }
        ] if get_project_data()['project_business_area'] else []
    }

    return {
        "target": get_create_project_target_param(),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_task_payload(dag_run,action,data):
    def get_custom_fileds_payload(task, action = False):
        custom_fileds = []

        def get_oef_payload(uri,value):
            custom_fileds.append(
                {
                "customField": {
                    "uri": uri,
                },
                "text": value
            })

        if task['task_business_area']:
            get_oef_payload(dag_run.conf['task_custom_fields']['task_business_area'],task['task_business_area'])

        if task['profit_center']:
            get_oef_payload(dag_run.conf['task_custom_fields']['profit_center'],task['profit_center'])

        if task['businessarea']:
            get_oef_payload(dag_run.conf['task_custom_fields']['wc_company_code'],task['businessarea'])
        
        return custom_fileds if action else custom_fileds[:-1] if custom_fileds else []
        
    status= {'1': 0, '2': 1, '3': 1, '4': 1}
    return list(map(lambda task: {
        "target": None if action == "add" else {"uri": task['uri']},
        "taskModificationToApply": {
                "name": task['taskcode'],
                "codeToApply": {
                    "value": task['taskname']
                },
                "isClosed": status[task['taskstatus']] if task['taskstatus'] and task['taskstatus'] in ['1','2','3','4'] else 0,
                "timeAndExpenseEntryTypeToApply": {
                    "value": "urn:replicon:time-and-expense-entry-type:billable"
                },
                "isTimeEntryAllowed": "1",
                "customFieldsToApply": get_custom_fileds_payload(task,'action') if task['businessarea'] and (task[
                    'company_code'] == '' if action == 'update' else True) else get_custom_fileds_payload(task),
                "resourceAssignmentModifications": {
                    "resourcesToAdd": [
                        {
                            "serviceCenter": {
                                "name": task['businessarea']
                            },
                        }
                    ] if task['businessarea'] and (task['company_code'] == '' if action == 'update' else True) else [],
                    "resourcesToRemove": []
                },
            }
    }, data))

def get_update_task_payload(dag_run):
    return {
        "project": {
            "uri": rail.result('create_project')['uri'],
        },
        "taskHierarchy": get_task_payload(dag_run,"update",rail.result("get_all_task_to_add_update")['tasks_to_update']),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_batch_put_task_payload(dag_run):
    return {
        "project": {
            "uri": rail.result('create_project')['uri'],
        },
        "taskHierarchy": get_task_payload(dag_run,"add",rail.result("get_all_task_to_add_update")['tasks_to_add']),
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

def get_exception_log_message():
    if not rail.result("get_service_center_details_for_project"):
        return 'companycode is not assigned to project as it is not present in replicon'
