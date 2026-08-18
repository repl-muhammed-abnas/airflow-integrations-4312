import uuid
import rail

def get_project_payload():
    return {
            "target": {
                "name": rail.result("for_each_issue_key")['projectname']
            },
            "projectInfo": {
                "name": rail.result("for_each_issue_key")['projectname'],
                "percentCompleted": "0",
                "isTimeEntryAllowed": "1",
                "isProjectLeaderApprovalRequired": "1"
        }
    }

def get_task_payload():
    project_data = rail.result('bulk_get_project_details')[0]['projectDetails']
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
                            "uri": project_data['uri'] if project_data else rail.result("create_project")['uri']
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
    project_data = rail.result('bulk_get_project_details')[0]['projectDetails']
    return {
            "task": {
                "target": {
                    "name": ((rail.result("for_each_issue_key")['taskname'])[slice(0,255)].strip()).replace('^"',"").replace('"$',"")
                },
                "name": ((rail.result("for_each_issue_key")['taskname'])[slice(0,255)].strip()).replace('^"',"").replace('"$',""),
                "code": rail.result("for_each_issue_key")['key'],
                "percentCompleted": "0",
                "isTimeEntryAllowed": "true",
                "isClosed": "false",
                "costTypeUri": "urn:replicon:cost-type:unclassified"
            },
            "unitOfWorkId": str(uuid.uuid4()),
            "project": {
                "uri": project_data['uri'] if project_data else rail.result("create_project")['uri']
            }
        }

def get_project_input_data():
    return {
            "projects": [
                {
                "name": rail.result("for_each_issue_key")['projectname']
                }
            ]
        }

def get_task_team_payload():
    return {
        "taskUri": rail.result("add_task")['uri'],
        "resourceUris": rail.result("get_all_project_tem_members"),
        "isAssigned": "true"
    }

def get_required_department(response):
    if not response:
        return []

    return list(filter(lambda item: item['name'] == 'GFG Tech', list(map(lambda item: {
        'name': item['displayText'],
        'uri': item['uri']
    },response))))

def get_project_team_assign_payload():
    return {
        "projectUri": rail.result("create_project")['uri'],
        "resourceUri": rail.result("get_enabled_departments"),
        "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
    }

def get_project_team_member_payload():
    project_data = rail.result('bulk_get_project_details')[0]['projectDetails']
    return {
        "projectUri": project_data['uri'] if project_data else rail.result("create_project")['uri']
    }
