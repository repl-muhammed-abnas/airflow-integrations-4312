import uuid
from datetime import datetime
import rail

INPUT_DATE_FORMAT = "%d.%m.%Y"


def get_conf():
    return rail.get_current_context()['dag_run'].conf


def get_project_detail_payload(dag_run, wbs_type):
    return {
        "projects": [
            {
                "name": dag_run.conf['wbs'] if wbs_type != "child" else dag_run.conf['child_wbs'],
            }
        ]
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
                "filterDefinitionUri": dag_run.conf['parent_wbs_filter_uri']
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['wbs'],
                },
                "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        }
    }


def get_remove_timeentry_payload():
    return {
        "target": {
            "uri": rail.result("get_project_details")['uri'],
        },
        "modifications": {
            "isTimeEntryAllowed": "false"
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_project_team_member_payload(dag_run, wbs_type):
    return {
        "projectUri": rail.result("get_project_details")['uri'] if wbs_type == "gsap" else
        dag_run.conf['parent_wbs_uri'] if wbs_type == "parent" else rail.result(
            "get_child_project_details")['uri'],
        "asOfDate": None
    }


def get_project_tasks_payload(wbs_type):
    return {
        "parentUri": rail.result("get_project_details" if wbs_type != "child" else "get_child_project_details")['uri'],
    }


def get_add_gsap_task_payload(dag_run):
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
                "parent": {
                    "uri": dag_run.conf['billingkey_task_uri'],
                },
                "parameterCorrelationId": None
            },
            "name": dag_run.conf['task_name'],
            "code": dag_run.conf['task_code'],
            "description": None,
            "timeEntryDateRange": {
                "startDate": get_payload_date(dag_run.conf['task_start_date']),
                "endDate": get_payload_date(dag_run.conf['task_end_date']),
                "relativeDateRangeUri": None,
                "relativeDateRangeAsOfDate": None
            },
            "percentCompleted": "0",
            "isTimeEntryAllowed": "true",
            "estimatedHours": None,
            "isClosed": "false",
            "customFieldValues": [{
                "customField": {
                        "uri": dag_run.conf['task_type_oef_uri'],
                        "groupUri": "urn:replicon:object-type:task"
                },
                "dropDownOption": {
                    "uri": dag_run.conf['gsap_task_option_uri'],
                },
            }
            ],
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
        date = datetime.strptime(date_str, INPUT_DATE_FORMAT)
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


def get_update_gsap_task_payload(dag_run):
    return {
        "target": {
            "uri": dag_run.conf['existing_task']['task_uri'],
        },
        "project": {
            "uri": dag_run.conf['project_uri'],
        },
        "modifications": {
            "name": None,
            "codeToApply": get_code_to_apply(dag_run.conf['task_code']),
            "descriptionToApply": None,
            "isClosed": "false",
            "timeEntryStartDateToApply": get_timeentry_date(dag_run.conf['task_start_date']),
            "timeEntryEndDateToApply": get_timeentry_date(dag_run.conf['task_end_date']),
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


def get_create_gsap_task_conf(dag_run):
    return {
        "file_name": dag_run.conf['file_name'],
        "project_name": dag_run.conf['project_name'],
        "project_uri": dag_run.conf['project_uri'],
        "project_startdate": dag_run.conf['project_startdate'],
        "project_enddate": dag_run.conf['project_enddate'],
        "task_name": dag_run.conf['task_name'],
        "task_code": dag_run.conf['task_code'],
        "task_start_date": dag_run.conf['task_start_date'],
        "task_end_date": dag_run.conf['task_end_date'],
        "user_list": dag_run.conf['user_list'],
        "billingkey_task_name": dag_run.conf['billingkey_task_name'],
        "billingkey_task_uri": dag_run.conf['billingkey_task_uri'],
        "task_type_oef_uri": dag_run.conf['task_type_oef_uri'],
        "gsap_task_option_uri": dag_run.conf['gsap_task_option_uri']
    }


def get_project_team_member_assignment_payload(dag_run, wbs_type):
    return {
        "taskUri": rail.result("create_gsapy_task" if wbs_type != "child" else "create_child_task")["uri"],
        "resourceUris": dag_run.conf['user_list'],
        "isAssigned": "true"
    }
