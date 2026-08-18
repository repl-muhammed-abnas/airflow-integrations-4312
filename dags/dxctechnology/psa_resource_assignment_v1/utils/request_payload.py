import rail

null = None


def get_process_each_wbs(item):
    return {
        'wbs': item['WBS'],
        'empid': item['PERN'],
        'assignmentStartDate': item['StartDate'],
        'assignmentEndDate': item['EndDate'],
        'useruri': get_name_uri_employeeid_status(item, "useruri"),
        'employeeid': get_name_uri_employeeid_status(item, "employeeid"),
        'companycode': get_name_uri_employeeid_status(item, "companycodefullpath")
    }


def get_process_each_child_wbs(item, dag_run):
    return {
        'wbs': item.split(" - ")[0].strip(),
        'empid': dag_run.conf['empid'],
        'assignmentStartDate': dag_run.conf['assignmentStartDate'],
        'assignmentEndDate': dag_run.conf['assignmentEndDate'],
        'useruri': dag_run.conf['useruri'],
        'companycode': dag_run.conf['companycode'],
        'parentWbs': dag_run.conf['wbs']
    }


def get_name_uri_employeeid_status(item, selection):
    ia_perner_id = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_active_user"), "employeeid", item['PERN'], selection)
    if bool(ia_perner_id):
        return ia_perner_id
    return null


def get_assignmentdaterange_payload(dag_run, task):
    assignment = rail.result('assignment_details')
    return {
        "projectUri": rail.result(task)["uri"],
        "resourceUri": dag_run.conf['useruri'],
        "dateRange": {
            "startDate": assignment['startDate'] if assignment['startDate'] else null,
            "endDate": assignment['endDate'] if assignment['endDate'] else null,
            "relativeDateRange": null,
            "relativeDateRangeAsOfDate": null}}


def get_assign_user_payload(dag_run, task):
    return {
        "projectUri": rail.result(task)["uri"],
        "resourceUri": dag_run.conf['useruri'],
        "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
    }


def get_child_wbs_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            rail.result('get_all_columns')[0]['uri']
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": rail.result('get_all_filter_defination')[0]['uri']
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
                    "text": dag_run.conf['wbs'],
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
