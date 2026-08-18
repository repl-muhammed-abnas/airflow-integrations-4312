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
    return {
        'wbs': item['wbs'],
        'empid': item['empid'],
        'tasklevel1': item['tasklevel1'],
        'assignmentStartDate': item['assignmentStartDate'],
        'assignmentEndDate': item['assignmentEndDate'],
        'divisions': rail.result('get_all_divisions'),
        'useruri': get_name_uri_employeeid_status(item, "useruri"),
        'employeeid': get_name_uri_employeeid_status(item, "employeeid"),
    }


def get_name_uri_employeeid_status(item, selection):
    employee_id = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_active_user"), "employeeid", item["empid"], selection)
    if bool(employee_id):
        return employee_id
    ia_perner_id = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_active_user"), "iapernerid", item['empid'], selection)
    if bool(ia_perner_id):
        return ia_perner_id
    perner = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_active_user"), "perner", item['empid'], selection)
    if bool(perner):
        return perner
    return null


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


def get_c1_compass_conf(dag_run):
    return {
        'empid': dag_run.conf['empid'],
        'wbs': dag_run.conf['wbs'],
        'assignmentStartDate': dag_run.conf['assignmentStartDate'],
        'assignmentEndDate': dag_run.conf['assignmentEndDate'],
        'useruri': dag_run.conf['useruri'],
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


def get_gsap_child_conf(dag_run, item):
    return {
        'empid': dag_run.conf['empid'],
        'wbs': dag_run.conf['wbs'],
        'date': rail.result("assignment_details"),
        'taskName': item['name'],
        'childTaskUri': item['uri'],
        'empUri': dag_run.conf['empUri'],
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


def get_update_date_to_parent_child_task(dag_run):
    return {
        "taskUri": dag_run.conf['childTaskUri'],
        "taskAllocations": [
            {
                "resourceUri": dag_run.conf['empUri'],
                "dateRange": dag_run.conf['date']
            }
        ]
    }
