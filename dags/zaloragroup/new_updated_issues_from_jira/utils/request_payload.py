from datetime import datetime
import uuid
import rail

def get_replicon_date(date_str, date_format='%Y-%m-%d'):
    if not date_str:
        return None
    # date format in 2006-04-01
    try:
        date = datetime.strptime(date_str, date_format)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None

#pylint: disable=invalid-character-backspace
def get_task_name(key, summary):
    taskname = ((key + " - " + (summary.replace("", "")))[slice(0,255)].strip()).replace('^"','').replace('"$',"")
    return taskname

def get_project_payload(department_name):
    return {
            "project": {
                "target": {
                    "name": f'Development - {get_replicon_date(rail.result("for_each_issue_key")["created"][0:10])["year"]}'
                },
                "projectInfo": {
                    "name": f'Development - {get_replicon_date(rail.result("for_each_issue_key")["created"][0:10])["year"]}',
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "true",
                    "isClosed": "0",
                    "costTypeUri": "urn:replicon:cost-type:unclassified",
                    "isProjectLeaderApprovalRequired": "true",
                    "billingTypeUri": "urn:replicon:billing-type:non-billable"
                },
                "tasks": [
                {
                    "task": {
                        "target": {
                            "name": get_task_name(rail.result("for_each_issue_key")['key'], rail.result(
                                            "for_each_issue_key")['summary'])
                        },
                        "name": get_task_name(rail.result("for_each_issue_key")['key'], rail.result(
                                            "for_each_issue_key")['summary']),
                        "code": rail.result("for_each_issue_key")['key'],
                        "percentCompleted": "0",
                        "isTimeEntryAllowed": "true",
                        "costTypeUri": "urn:replicon:cost-type:unclassified"
                    }
                }
                ],
                "team": {
                    "teamMembers": [
                        {
                        "resource": {
                            "department": {
                                "name": department_name
                            }
                        }
                    }
                ]
            }
        }
    }

def get_task_payload():
    return {
            "columnUris": [
                "urn:replicon:task-list-column:task",
                "urn:replicon:task-list-column:code"
            ],
            "page": "1",
            "pagesize": "1000",
            "filterExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:task-list-filter:project"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "uri": rail.result('bulk_get_project_details')[0]['projectDetails']['uri']
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:task-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": rail.result("for_each_issue_key")['key']
                        }
                    }
                }
            }
        }

def create_task_payload():
    return {
            "task": {
                "target": {
                    "name": get_task_name(rail.result("for_each_issue_key")['key'], rail.result("for_each_issue_key")['summary'])
                },
                "name": get_task_name(rail.result("for_each_issue_key")['key'], rail.result("for_each_issue_key")['summary']),
                "code": rail.result("for_each_issue_key")['key'],
                "percentCompleted": "0",
                "isTimeEntryAllowed": "true",
                "isClosed": "false",
                "costTypeUri": "urn:replicon:cost-type:unclassified"
            },
            "unitOfWorkId": str(uuid.uuid4()),
            "project": {
                "uri": rail.result('bulk_get_project_details')[0]['projectDetails']['uri']
            }
        }

def get_project_input_data():
    return {
            "projects": [
                {
                "name": f'Development - {get_replicon_date(rail.result("for_each_issue_key")["created"][0:10])["year"]}'
                }
            ]
        }

def get_task_team_payload():
    return {
        "taskUri": rail.result("add_task")['uri'],
        "resourceUris": rail.result("get_all_project_tem_members"),
        "isAssigned": "true"
    }
