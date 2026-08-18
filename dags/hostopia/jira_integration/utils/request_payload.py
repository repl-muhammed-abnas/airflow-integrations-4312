import uuid
import rail
from hostopia.jira_integration.utils.custom_method import get_replicon_date


def get_data_for_program():
    return {
        "program": {
            "target": {
                "name": rail.result('board_check')[0]['location']['projectName']
            },
            "name": rail.result('board_check')[0]['location']['projectName'],
            "programManager": {
                "uri": rail.result("search_user_in_replicon")[0]['uri'] if rail.result("search_user_in_replicon") else None
            },
            "isActive": "1"
        }
    }


def get_user_data_payload(dag_run,item):
    return {
        "page": "1",
        "pagesize": "1111",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            dag_run.conf['column_uri']
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": dag_run.conf['filter_uri']
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": f'{item}'
                }
            }
        }
    }


def get_dates(data):
    return {
        "year": data.strftime("%Y"),
        "month": data.strftime("%m"),
        "day": data.strftime("%d")
    }


def get_project_creation_payload(dag_run):
    return {
        "project": {
            "target": {
                "name": dag_run.conf['summary']
            },
            "projectInfo": {
                "name": dag_run.conf['summary'],
                "code": dag_run.conf['Key'],
                "timeEntryDateRange": {
                    "startDate": get_replicon_date(dag_run.conf['startdate'], "%Y-%m-%d"),
                    "endDate": get_replicon_date(dag_run.conf['enddate'], "%Y-%m-%d")
                },
                "projectStatusLabel": {
                    "name": "In Progress"
                },
                "percentCompleted": "0",
                "program": {
                    "name": dag_run.conf['programname']
                },
                "isTimeEntryAllowed": "1",
                "isProjectLeaderApprovalRequired": "0",
            }
        }
    }


def get_task_payload(item, dag_run, update_action_type='add'):
    is_triggered_by_update_project = bool(
        rail.result('serach_project_in_replicon')['uri'])
    project_uri = rail.result('serach_project_in_replicon')['uri']
    return {
        "project": {
            "uri": project_uri
        },
        "taskHierarchy": get_payload_by_item_and_dag_run(item, project_uri, dag_run, is_triggered_by_update_project, update_action_type),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_payload_by_item_and_dag_run(item, project_uri, dag_run, is_triggered_by_update_project, update_action_type):
    if is_triggered_by_update_project:
        if update_action_type == 'update':
            status = dag_run.conf['status'] if dag_run.conf['status'] == "Done" else item['status'] if item['status'] == "Done" else None
            return [
                {
                    "target": {
                        "name": item['taskname'],
                        "project": {
                            "uri": project_uri
                        },
                    },
                    "taskModificationToApply": {
                        "isClosed": 1 if status else 0,
                        "timeEntryStartDateToApply": {
                            "date": get_replicon_date(item['startdate'], "%Y-%m-%d")
                        },
                        "timeEntryEndDateToApply": {
                            "date": get_replicon_date(item['enddate'], "%Y-%m-%d")
                        }
                    }
                }
            ]
    return [
        {
            "taskModificationToApply": {
                "name": item['taskname'],
                "codeToApply": {
                    "value": item['taskcode']
                },
                "isClosed": 1 if item['status'] == "Done" else 0,
                "timeEntryStartDateToApply": {
                    "date": get_replicon_date(item['startdate'], "%Y-%m-%d")
                },
                "timeEntryEndDateToApply": {
                    "date": get_replicon_date(item['enddate'], "%Y-%m-%d")
                },
                "isTimeEntryAllowed": 1,
            }
        }
    ]


def get_task_create_payload(item):
    project_uri = rail.result('create_project_in_replicon')['uri']
    status = item['fields']['status']['name'] if item['fields']['status']['name'] == "Done" else None
    return {
        "project": {
            "uri": project_uri
        },
        "taskHierarchy": [
            {
                "taskModificationToApply": {
                    "name": item['fields']['summary'],
                    "codeToApply": {
                        "value": item['key']
                    },
                    "isClosed": 1 if status else 0,
                    "timeEntryStartDateToApply": {
                        "date": get_replicon_date(item['fields']['customfield_10037'], "%Y-%m-%d") if item[
                            'fields']['customfield_10037'] else None
                    },
                    "timeEntryEndDateToApply": {
                        "date": get_replicon_date(item['fields']['customfield_10034'], "%Y-%m-%d") if item[
                            'fields']['customfield_10034'] else None
                    },
                    "isTimeEntryAllowed": 1
                }
            }
        ],
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def resource_uri_payload():
    resource_uris=[]
    for item in rail.result("get_user_list"):
        if item:
            resource_uris.append(item['uri'])
    return {
        "projectUri": rail.result('serach_project_in_replicon')['uri'] if rail.result(
            'serach_project_in_replicon') else rail.result('create_project_in_replicon')['uri'],
        "resourceUris": resource_uris
    }


def update_date_payload(dag_run):
    return {
        "projectUri": rail.result('serach_project_in_replicon')["uri"],
        "dateRange": {
            "startDate": get_replicon_date(dag_run.conf['startdate'], "%Y-%m-%d"),
            "endDate": get_replicon_date(dag_run.conf['enddate'], "%Y-%m-%d"),
        }
    }
