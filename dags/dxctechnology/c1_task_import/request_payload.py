import uuid
from datetime import datetime
import rail


def get_conf():
    return rail.get_current_context()['dag_run'].conf


def get_project_detail_payload(dag_run, wbs_type):
    return {
        "projects": [
            {
                "uri": None,
                "name": dag_run.conf['wbs'] if wbs_type != "compass" else dag_run.conf['child_wbs'],
                "code": None,
                "parameterCorrelationId": None
            }
        ]
    }


def get_remove_timeentry_payload():
    return {
        "target": {
            "uri": rail.result("get_project_details")['uri'],
            "name": None,
            "code": None,
            "parameterCorrelationId": None
        },
        "modifications": {
            "isTimeEntryAllowed": "false"
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_project_team_member_payload(dag_run, wbs_type):
    return {
        "projectUri": rail.result("get_project_details")['uri'] if wbs_type == "c1" else
        dag_run.conf['parent_wbs_uri'] if wbs_type == "parent" else rail.result(
            "get_compass_project_details")['uri'],
        "asOfDate": None
    }


def get_project_tasks_payload(wbs_type):
    return {
        "parentUri": rail.result("get_project_details" if wbs_type != "compass" else "get_compass_project_details")['uri'],
    }


def get_add_c1_task_payload(dag_run):
    return {
        "project": {
            "uri": dag_run.conf['project_uri'],
            "name": None,
            "code": None,
            "parameterCorrelationId": None
        },
        "task": {
            "target": {
                "uri": None,
                "name": dag_run.conf['task_name'],
                "parent": None,
                "parameterCorrelationId": None
            },
            "name": dag_run.conf['task_name'],
            "code": dag_run.conf['task_code'],
            "description": None,
            "timeEntryDateRange": {
                "startDate": get_payload_date(dag_run.conf['start_date']),
                "endDate": get_payload_date(dag_run.conf['end_date']),
                "relativeDateRangeUri": None,
                "relativeDateRangeAsOfDate": None
            },
            "percentCompleted": "0",
            "isTimeEntryAllowed": "true",
            "estimatedHours": None,
            "isClosed": "false",
            "customFieldValues": [],
            "estimatedCost": None,
            "costTypeUri": None,
            "timeAndExpenseEntryTypeUri": None,
            "assignedResources": [],
            "keyValues": [],
            "historicalKeyValues": [],
            "extensionFieldValues": []
        }}


def get_code_to_apply(task_code):
    return {
        "value": task_code,
    } if task_code else None


def get_payload_date(date_str):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, '%d/%m/%Y')
        return {
            "year": date.year,
            "month": date.month,
            "day": date.day
        }
    except Exception as e:
        raise Exception(f"Invalid date received. Date:{date_str}") from e


def get_timeentry_date(date):
    return {
        "date": get_payload_date(date)
    } if date else None


def get_update_c1_task_payload(dag_run):
    return {
        "target": {
            "uri": dag_run.conf['existing_tasks']['uri'],
            "name": None,
            "parent": None,
            "parameterCorrelationId": None
        },
        "project": {
            "uri": dag_run.conf['project_uri'],
            "name": None,
            "code": None,
            "parameterCorrelationId": None
        },
        "modifications": {
            "name": None,
            "codeToApply": get_code_to_apply(dag_run.conf['task_code']),
            "descriptionToApply": None,
            "isClosed": "false",
            "timeEntryStartDateToApply": get_timeentry_date(dag_run.conf['start_date']),
            "timeEntryEndDateToApply": get_timeentry_date(dag_run.conf['end_date']),
            "timeAndExpenseEntryTypeToApply": None,
            "isTimeEntryAllowed": "true",
            "costTypeToApply": None,
            "estimatedHoursToApply": None,
            "estimatedCostToApply": None,
            "resourceAssignmentModifications": None,
            "customFieldsToApply": [],
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": []
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_create_c1_task_conf(dag_run):
    return {
        "project_name": dag_run.conf['project_name'],
        "project_uri": dag_run.conf['project_uri'],
        "project_startdate": dag_run.conf['project_startdate'],
        "project_enddate": dag_run.conf['project_enddate'],
        "task_name": dag_run.conf['task_name'],
        "task_code": dag_run.conf['task_code'],
        "start_date": dag_run.conf['start_date'],
        "end_date": dag_run.conf['end_date'],
        "user_list": dag_run.conf['user_list'],
    }


def get_project_team_member_assignment_payload(dag_run, wbs_type):
    return {
        "taskUri": rail.result("create_c1_task" if wbs_type != "compass" else "create_compass_task")["uri"],
        "resourceUris": dag_run.conf['user_list'],
        "isAssigned": "true"
    }


def get_getdata_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            dag_run.conf["parent_wbs_column_uri"]
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": None,
                "filterDefinitionUri": dag_run.conf['parent_wbs_filter_uri']
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": {
                    "uri": None,
                    "uris": [],
                    "bool": None,
                    "date": None,
                    "money": None,
                    "number": None,
                    "text": dag_run.conf['wbs'],
                    "time": None,
                    "calendarDayDurationValue": None,
                    "workdayDurationValue": None,
                    "dateRange": None,
                    "dateTimeUtc": None,
                    "dateTimeUtcRange": None,
                    "numberRange": None
                },
                "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        }
    }
