import rail
from partslifeinc.time_entry.utils import python_callable_methods


null=None

def get_break_punch_in_to_payload(dag_run):
    payload = {
                "timePunch": {
                    "target": {
                    "parameterCorrelationId": null,
                    "uri": null,
                    "slug": null
                    },
                    "user": {
                    "uri": rail.result('search_user'),
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                    },
                    "punchTime": {
                    "year": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["year"],
                    "month": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["month"],
                    "day": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["day"],
                    "hour":  rail.result('for_each_record_start')["punch_in_hr"],
                    "minute": rail.result('for_each_record_start')["punch_in_min"],
                    "second": "1",
                    "timeZoneUri": null
                    },
                    "actionUri": "urn:replicon:time-punch-action:start-break",
                    "punchInAttributes": null,
                    "punchStartBreakAttributes": {
                    "breakType": {
                        "uri": rail.result('get_break_type_uri'),
                        "name": null
                    }
                    },
                    "extensionFieldValues":[],
                    "rawTimePunchUri": null
                },
                "audit": null
                }
    if rail.result('for_each_record_start')["attendance_code"]:
        payload["timePunch"]["extensionFieldValues"].append(
            {
                "definition": {
                "uri": rail.result('get_timeentry_oef')['attendance_code'],
                "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue":  rail.result('for_each_record_start')["attendance_code"],
                "fileValue": null,
                "jsonValue": null
            }
        )

    if rail.result('for_each_record_start')["end_item"]:
        payload["timePunch"]["extensionFieldValues"].append(
           {
                "definition": {
                "uri":  rail.result('get_timeentry_oef')['end_item'],
                "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue":  rail.result('for_each_record_start')["end_item"],
                "fileValue": null,
                "jsonValue": null
            }
        )

    return payload

def get_break_punch_out_to_payload(dag_run):
    payload = {
                "timePunch": {
                    "target": {
                    "parameterCorrelationId": null,
                    "uri": null,
                    "slug": null
                    },
                    "user": {
                    "uri": rail.result('search_user'),
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                    },
                    "punchTime": {
                    "year": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["year"],
                    "month": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["month"],
                    "day": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["day"],
                    "hour":  rail.result('for_each_record_start')["punch_out_hr"],
                    "minute": rail.result('for_each_record_start')["punch_out_min"],
                    "second": "0",
                    "timeZoneUri": null
                    },
                    "actionUri": "urn:replicon:time-punch-action:out",
                    "punchInAttributes": null,
                    "punchStartBreakAttributes": null,
                    "extensionFieldValues":[],
                    "rawTimePunchUri": null
                },
                "audit": null
                }

    return payload

def get_punch_in_payload(dag_run):
    payload =  {
                "timePunch": {
                    "target": {
                    "parameterCorrelationId": null,
                    "uri": null,
                    "slug": null
                    },
                    "user": {
                    "uri": rail.result('search_user'),
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                    },
                    "punchTime": {
                    "year": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["year"],
                    "month": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["month"],
                    "day": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["day"],
                    "hour":  rail.result('for_each_record_start')["punch_in_hr"],
                    "minute": rail.result('for_each_record_start')["punch_in_min"],
                    "second": "1",
                    "timeZoneUri": null
                    },
                    "actionUri": "urn:replicon:time-punch-action:in",
                    "punchInAttributes": null,
                    "punchStartBreakAttributes": null,
                    "extensionFieldValues":[],
                    "rawTimePunchUri": null
                },
                "audit": null
                }
    if rail.result('for_each_record_start')["attendance_code"]:
        payload["timePunch"]["extensionFieldValues"].append(
            {
                "definition": {
                "uri": rail.result('get_timeentry_oef')['attendance_code'],
                "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue":  rail.result('for_each_record_start')["attendance_code"],
                "fileValue": null,
                "jsonValue": null
            }
        )

    if rail.result('for_each_record_start')["end_item"]:
        payload["timePunch"]["extensionFieldValues"].append(
           {
                "definition": {
                "uri":  rail.result('get_timeentry_oef')['end_item'],
                "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue":  rail.result('for_each_record_start')["end_item"],
                "fileValue": null,
                "jsonValue": null
            }
        )
    return payload

def get_punch_out_payload(dag_run):
    payload = {
                "timePunch": {
                    "target": {
                    "parameterCorrelationId": null,
                    "uri": null,
                    "slug": null
                    },
                    "user": {
                    "uri": rail.result('search_user'),
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                    },
                    "punchTime": {
                    "year": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["year"],
                    "month": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["month"],
                    "day": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["day"],
                    "hour":  rail.result('for_each_record_start')["punch_out_hr"],
                    "minute": rail.result('for_each_record_start')["punch_out_min"],
                    "second": "0",
                    "timeZoneUri": null
                    },
                    "actionUri": "urn:replicon:time-punch-action:out",
                    "punchInAttributes": null,
                    "punchStartBreakAttributes": null,
                    "extensionFieldValues":[],
                    "rawTimePunchUri": null
                },
                "audit": null
                }
    return payload

def get_punch_in_payload_with_project_details(dag_run):
    payload = {
      "timePunch": {
        "target": {
          "parameterCorrelationId": null,
          "uri": null,
          "slug": null
        },
        "user": {
          "uri": rail.result('search_user'),
          "loginName": null,
          "employeeId": null,
          "parameterCorrelationId": null
        },
        "punchTime": {
            "year": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["year"],
            "month": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["month"],
            "day": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["day"],
            "hour":  rail.result('for_each_record_start')["punch_in_hr"],
            "minute": rail.result('for_each_record_start')["punch_in_min"],
            "second": "1",
            "timeZoneUri": null
        },
        "actionUri": "urn:replicon:time-punch-action:in",
        "punchInAttributes": null,
        "punchStartBreakAttributes": null,
        "extensionFieldValues": [],
        "rawTimePunchUri": null
      },
      "audit": null
    }

    if rail.result('log_subtask_uri'):
        payload["timePunch"]["punchInAttributes"] = {
        "activity": null,
          "project": {
            "uri": rail.result('get_project_details')['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
          },
          "client": null,
          "task": {
            "uri": rail.result('log_subtask_uri'),
            "name": null,
            "parent": {
              "uri": rail.result('log_task_uri'),
              "name": null,
              "parent": null,
              "parameterCorrelationId": null
            },
            "parameterCorrelationId": null
          },
          "billingRate": null,
          "isBillable": null
        }
    elif rail.result('log_task_uri') and not rail.result('log_subtask_uri'):
        payload["timePunch"]["punchInAttributes"] = {
        "activity": null,
          "project": {
            "uri": rail.result('get_project_details')['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
          },
          "client": null,
          "task": {
            "uri": rail.result('log_task_uri'),
            "name": null,
            "parent": null,
            "parameterCorrelationId": null
          },
          "billingRate": null,
          "isBillable": null
        }
    else:
        payload["timePunch"]["punchInAttributes"] = {
        "activity": null,
          "project": {
            "uri": rail.result('get_project_details')['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
          },
          "client": null,
          "task": null,
          "billingRate": null,
          "isBillable": null
        }
    if rail.result('for_each_record_start')["attendance_code"]:
        payload["timePunch"]["extensionFieldValues"].append(
            {
                "definition": {
                "uri": rail.result('get_timeentry_oef')['attendance_code'],
                "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue":  rail.result('for_each_record_start')["attendance_code"],
                "fileValue": null,
                "jsonValue": null
            }
        )

    if rail.result('for_each_record_start')["end_item"]:
        payload["timePunch"]["extensionFieldValues"].append(
           {
                "definition": {
                "uri":  rail.result('get_timeentry_oef')['end_item'],
                "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue":  rail.result('for_each_record_start')["end_item"],
                "fileValue": null,
                "jsonValue": null
            }
        )
    return payload

def get_punch_out_payload_with_project_details(dag_run):
    payload = {
      "timePunch": {
        "target": {
          "parameterCorrelationId": null,
          "uri": null,
          "slug": null
        },
        "user": {
          "uri": rail.result('search_user'),
          "loginName": null,
          "employeeId": null,
          "parameterCorrelationId": null
        },
        "punchTime": {
            "year": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["year"],
            "month": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["month"],
            "day": python_callable_methods.validate_date_format(dag_run.conf['timesheet_entry_date'])["day"],
            "hour":  rail.result('for_each_record_start')["punch_out_hr"],
            "minute": rail.result('for_each_record_start')["punch_out_min"],
            "second": "0",
            "timeZoneUri": null
        },
        "actionUri": "urn:replicon:time-punch-action:out",
        "punchInAttributes": null,
        "punchStartBreakAttributes": null,
        "extensionFieldValues": [],
        "rawTimePunchUri": null
      },
      "audit": null
    }

    return payload

def get_search_user_payload(dag_run):
    return {
                    "page": "1",
                    "pagesize": "100",
                    "columnUris": [
                        "urn:replicon:user-list-column:user",
                        "urn:replicon:user-list-column:login-name",
                        "urn:replicon:user-list-column:employee-id",
                        "urn:replicon:user-list-column:enabled"
                    ],
                    "sort": [],
                    "filterExpression": {
                        "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
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
                            "text": dag_run.conf["employee"],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    }
                    }
