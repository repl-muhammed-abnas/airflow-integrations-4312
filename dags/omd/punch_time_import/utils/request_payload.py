from uuid import uuid4
import rail

null=None

FEED_ENTRYDATE_DATE_FORMAT = "%m/%d/%Y"
FEED_ENTRYTIME_TIME_FORMAT = "%H:%M:%S"

PUNCH_IN = "punch_in"
PUNCH_OUT = "punch_out"

def get_user_data_payload(dag_run):
    return {
        "users": [
            {
            "uri": null,
            "loginName": null,
            "employeeId": dag_run.conf['employee_id'],
            "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_all_timesheet_for_user():
    max_min_date_for_user = rail.result('max_min_date_for_user')
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:timesheet-list-column:timesheet-status",
            "urn:replicon:timesheet-list-column:timesheet",
            "urn:replicon:timesheet-list-column:timesheet-period",
            "urn:replicon:timesheet-list-column:timesheet-owner"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-period-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                    "dateRange": {
                            "startDate": rail.parse_date(max_min_date_for_user[0], FEED_ENTRYDATE_DATE_FORMAT),
                            "endDate": rail.parse_date(max_min_date_for_user[1], FEED_ENTRYDATE_DATE_FORMAT)
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-owner"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                    "uri": rail.result('get_user_uri')
                    }
                }
            }
        }
    }

def get_time_punches(feed_punch_data):
    user_uri = rail.result('get_user_uri')
    action_uris = {
        PUNCH_IN : "urn:replicon:time-punch-action:in",
        PUNCH_OUT : "urn:replicon:time-punch-action:out",
    }
    response = []
    for entry_date, punch_data in feed_punch_data.items():
        for action, time in punch_data.items():
            if punch_data.get(action):
                punch_time = rail.parse_date(entry_date, FEED_ENTRYDATE_DATE_FORMAT)
                time = time.split(":")
                punch_time['hour'] = int(time[0])
                punch_time['minute'] = int(time[1])
                punch_time['second'] = 0
                response.append(
                    {
                        "timePunch": {
                            "target": null,
                            "user": {
                            "uri": user_uri
                            },
                            "punchTime": punch_time,
                            "actionUri": action_uris[action],
                            "extensionFieldValues": [],
                        }
                    }
                )
    return response


def add_bulk_punches_payload():
    feed_punch_data = rail.result("punch_in_out_for_each_date")
    return {
        "timePunches": get_time_punches(feed_punch_data),
        "bulkPutTimePunchBehaviour": {
            "bulkPutTimePunchBehaviourErrorHandlingOptionUri": "urn:replicon:bulk-put-time-punch-behaviour-error-handling-option:keep-partial-modifications"
        },
        "unitOfWorkId": str(uuid4())
    }

