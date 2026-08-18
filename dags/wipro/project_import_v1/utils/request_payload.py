from datetime import datetime as dt
import uuid
import rail
from rail import get_current_context

def does_wbs_exist():
    return bool(rail.result('get_project_details'))

def get_task_state(task_id):
    task_instance = get_current_context()['dag_run'].get_task_instance(task_id)
    return task_instance.current_state() if task_instance else None

def get_log_message():
    msg =''
    if get_task_state("log_user_skipped").lower() == "success":
        msg += ', project manager is not synced since the user details are not received from the payload'

    if does_wbs_exist():
        return "Project Updated Successfully" + msg
    return "Project Added Successfully" + msg

def get_create_project_target_param():
    if does_wbs_exist():
        return {
            "uri": rail.result('get_project_details')['uri']
        }
    return None

def create_projectorapply_modifications(dag_run):
    get_project_data = rail.result("load_project_data_from_query")
    modifications = {
        "nameToApply": {
            "value": get_project_data["projectname"]
        },
        "codeToApply":  {
            "value": get_project_data["projectcode"]
        } if not does_wbs_exist() else None,
        "descriptionToApply": {
            "value": get_project_data["projectname"]
        } if get_project_data["projectname"] else None,
        "billingTypeToApply": {
            "value": "urn:replicon:billing-type:time-and-material"
        },
        "timeAndMaterials": {
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
            "billingRates": [{
                "billingRate": {
                    "uri": "urn:replicon:project-specific-billing-rate"
                }
            }]
        },
        "statusToApply": {
            "name": 'In Progress'
        },
        "isTimeEntryAllowed": "0",
        "customFieldsToApply": [
            {
                "customField": {
                    "uri": dag_run.conf['last_modified_date_udf_uri']
                },
                "date": rail.get_replicon_date(dt.now())
            }
        ],
        "objectExtensionFieldsToApply": [
            {
                "definition": {
                    "uri": dag_run.conf['project_export_type_oef'],
                },
                "tag": {
                    "uri": dag_run.conf['it_proj_details_dropdown_uri']
                }
            }
        ] if not does_wbs_exist() else []
    }

    return {
        "target": get_create_project_target_param(),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def format_project_task_details(response):
    return list(map(lambda task: {
        "task_name": task['name'],
        "task_code": task['code'],
        "uri": task['uri']
    }, response))

def get_create_user_payload(dag_run):
    user_data = rail.result("load_project_data_from_query")
    return {
        "user": {
            "target": {
                "loginName": user_data["pm_loginname"]+'@wipro.com'
            },
            "firstname": user_data['pm_name'].split(' ')[0],
            "lastname": user_data['pm_name'][len(user_data['pm_name'].split(' ')[0])+1:],
            "emailAddress": user_data['pm_email'],
            "employeeId": user_data['pm_empid'],
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "1",
                "loginName": user_data["pm_loginname"]+'@wipro.com',
                "SSOName": user_data["pm_loginname"]+ '@wipro.com',
            },
            "employeeType": {
                "uri": dag_run.conf["employeetypeuri"]
            } if dag_run.conf["employeetypeuri"] else None,
            "permissionSets": [
                {
                    "name": 'Project Manager'
                },
                {
                    "name": 'L1 Manager'
                },
                {
                    "name": 'End User (Managers)'
                }
            ],
        }
    }

def get_add_task_payload(data):
    payload=[]
    for item in data:
        all_records = list(item.values())
        task_details = all_records[0][0]
        payload.append({
        "target": None,
        "taskModificationToApply": {
                "name": task_details['taskname'],
                "codeToApply": {
                    "value": task_details['taskcode']
                },
                "isClosed": 0,
                "timeAndExpenseEntryTypeToApply": {
                    "value": "urn:replicon:time-and-expense-entry-type:billable"
                },
                "isTimeEntryAllowed": "1",
                "resourceTaskAssignmentModifications": {
                    "resourceAllocationsToAdd": list(map( lambda res :
                        {
                        "resource": {
                            "user": { "uri": res['user_uri'] }
                        },
                        "dateRange": {
                            "startDate": rail.parse_date(res['taskstartdate'],'%Y%m%d'),
                            "endDate": rail.parse_date(res['taskenddate'],'%Y%m%d')
                        }
                        }
                    , all_records[0]))
                }
            }
    })

    return payload

def get_task_payload(action,data):
    return list(map(lambda task: {
        "target": None if action == "add" else {"uri": task['uri']},
        "taskModificationToApply": {
                "name": task['taskname'],
                "codeToApply": {
                    "value": task['taskcode']
                },
                "isClosed": 0,
                "timeAndExpenseEntryTypeToApply": {
                    "value": "urn:replicon:time-and-expense-entry-type:billable"
                },
                "isTimeEntryAllowed": "1",
                "resourceTaskAssignmentModifications": {
                    "resourceAllocationsToAdd": [
                        {
                        "resource": {
                            "user": { "uri": task['user_uri'] }
                        },
                        "dateRange": {
                            "startDate": task['assignment_start_date'] if task['assignment_start_date'] else rail.parse_date(task[
                                'taskstartdate'],'%Y%m%d'),
                            "endDate": rail.parse_date(task['taskenddate'],'%Y%m%d')
                        }
                        }
                    ]
                }
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
        "taskHierarchy": get_add_task_payload(rail.result("get_all_task_to_add_update")['tasks_to_add']),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_foreign_managers_payload():
    return {
            "page": "1",
            "pagesize": "100000",
            "columnUris": [
                "urn:replicon:user-list-column:user",
                "urn:replicon:user-list-column:login-name"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:employee-type"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "uri": rail.result('get_foreign_manager_employee_type_details')
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

def get_projects_for_user_payload():
    return {
            "page": "1",
            "pagesize": "100000",
            "columnUris": [
                "urn:replicon:project-list-column:project"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:project-list-filter:project-leader"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                    "uri": rail.result('for_each_user')['uri']
                    }
                }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:project-list-filter:status"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                    "uri": "urn:replicon:project-status-type:in-progress"
                    }
                }
            }
        }
    }
