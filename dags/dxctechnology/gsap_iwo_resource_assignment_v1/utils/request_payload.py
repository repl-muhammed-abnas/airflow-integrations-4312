import rail

null = None


def get_all_division_payload():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:division-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
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


def get_process_each_wbs(item):
    """
    Pass grouped WBS item with all employees for that WBS.
    This reduces API calls by processing multiple employees per WBS in single child DAG.
    """
    return {
        'wbs': item['wbs'],
        'tasklevel1': item['tasklevel1'],
        'employees': item['employees'],  # List of all employees for this WBS
        'divisions': rail.result('get_all_divisions'),
    }


def get_employee_details_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
                "urn:replicon:user-list-column:user",
                "urn:replicon:user-list-column:employee-id"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['empid']
                }
            }
        }
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


def get_assignmentdaterange_payload(dag_run):
    assignment = rail.result('assignment_details')
    return {
        "projectUri": rail.result("get_project_info_based_on_wbs_element")[0]["projectDetails"]["uri"],
        "resourceUri": dag_run.conf['useruri'],
        "dateRange": {
            "startDate": assignment['startDate'] if assignment['startDate'] else null,
            "endDate": assignment['endDate'] if assignment['endDate'] else null,
            "relativeDateRange": null,
            "relativeDateRangeAsOfDate": null}}


def get_assign_user_payload(dag_run):
    return {
        "projectUri": rail.result("get_project_info_based_on_wbs_element")[0]["projectDetails"]["uri"],
        "resourceUri": dag_run.conf['useruri'],
        "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
    }


def get_put_key_value_project():
    return {
        "projectUri": rail.result("get_project_info_based_on_wbs_element")[0]["projectDetails"]["uri"],
        "keyValue": {"keyUri": "urn:replicon:project-key-value-key:project-team-member-assignment-type",
                     "value": {"uri": "urn:replicon:project-team-member-assignment-type:manually-assign-task"}
                     }
    }


def get_c1_compass_conf(dag_run, item):
    """Pass employee details from ForEach loop to C1/Compass assignment DAG"""
    return {
        'empid': item['empid'],
        'wbs': dag_run.conf['wbs'],
        'assignmentStartDate': item['assignmentStartDate'],
        'assignmentEndDate': item['assignmentEndDate'],
        'useruri': item['useruri'],
        'taskName': dag_run.conf['tasklevel1'],
        'log_artifact': rail.result("create_log")
    }


def get_gsap_conf(dag_run, item):
    return {
        'empid': dag_run.conf['empid'],
        'wbs': dag_run.conf['wbs'],
        'assignmentStartDate': dag_run.conf['assignmentStartDate'],
        'assignmentEndDate': dag_run.conf['assignmentEndDate'],
        'taskName': item['name'],
        'parentTaskUri': item['uri'],
        'empUri': dag_run.conf['useruri'],
        'log_artifact': dag_run.conf["log_artifact"]
    }


def get_update_date_to_parent_task(dag_run):
    return {
        "taskUri": dag_run.conf['parentTaskUri'],
        "taskAllocations": [
            {
                "resourceUri": dag_run.conf['empUri'],
                "dateRange": rail.result("assignment_details")
            }
        ]
    }


def get_update_date_to_child_task_inline():
    """
    Inline child task update - used with ForEachOperator instead of separate child DAG.
    Gets child task URI from ForEach result and empUri/dateRange from dag_run.conf.
    """
    child_task = rail.result('for_each_child_task')
    return {
        "taskUri": child_task['uri'],
        "taskAllocations": [
            {
                "resourceUri": rail.get_current_context()['dag_run'].conf['empUri'],
                "dateRange": rail.result("assignment_details")
            }
        ]
    }
