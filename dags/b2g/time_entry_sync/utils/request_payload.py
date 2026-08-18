from datetime import datetime
import uuid
import rail
null = None


def get_process_time_data_records_conf(item):
    return {
        **{k: v if v is not None else '' for k, v in item.items()}
    }


def mandatory_fields_check(dag_run):
    return (dag_run.conf['Entry_Date'] and dag_run.conf['User_Name'] and dag_run.conf['Hours']
            and dag_run.conf['Project_Name'] and (dag_run.conf['Task_Code']))


def get_search_user_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:login-name"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['User_Name']
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_task_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
                "urn:replicon:task-list-column:task",
                "urn:replicon:task-list-column:enabled",
                "urn:replicon:task-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:task-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf['Task_Code'],
                    },
                },
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:task-list-filter:project"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "uri": rail.result('get_project_details')['uri'],
                    },
                },
            },
        }
    }


def get_time_entry_payload(dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['Entry_Date'], '%d/%m/%Y')

    return {
        "timeEntryRevisionGroup": {
            "user": {
                "uri": rail.result('search_user')[0]['uri']
            },
            "entryDate": {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            },
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:attendance",
                "urn:replicon:time-allocation-type:project"
            ],
            "interval": {
                "hours": {
                    "hours": "0",
                    "minutes": "0",
                    "seconds": int(float(dag_run.conf['Hours']) * 3600),
                    "milliseconds": "0",
                    "microseconds": "0"
                }
            },
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:task",
                    "value": {
                        "uri": rail.result('get_task_data')[0]['Taskuri']
                    }
                },
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:comments",
                    "value": {
                        "text": dag_run.conf['Comment']
                    }
                }
            ]
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_submit_timesheet_payload(dag_run):
    timesheetlist = set(list(map(lambda item: item['timesheeturi'], dag_run.conf['timesheetdetails'])))
    return {
        "timesheetUris": list(filter(None,list(timesheetlist))),
        "comments": "Submitted by integration",
        "submitOptions": []
    }


def get_timesheet_for_date(dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['Entry_Date'], '%d/%m/%Y')

    return {
        "userUri": rail.result('search_user')[0]['uri'],
        "date": {
            "year": effective_date.year,
            "month": effective_date.month,
            "day": effective_date.day
        },
        "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
    }
