"""
Request payload builders for Azenta Oracle PPM Time Export Integration (FI017)
"""
from airflow.exceptions import AirflowFailException
import rail

null = None

# Replicon's TimeDataExportService allows only one of these two approval filters per export.
# Keyed by config.time_export_approval_filter_mode — swap the config value to change filters
# without touching this code, matching the reference pattern in
# dags/capgemini/time_export_global_v8/utils/request_payload.py (hardcoded to "timesheet" there).
APPROVAL_FILTER_URI_BY_MODE = {
    "timesheet": "urn:replicon:time-data-export-filter:timesheet-only-approval-status",
    "time_entry": "urn:replicon:time-data-export-filter:time-entry-approval-status",
}


def _build_approval_and_entry_type_clause(approval_filter_mode):
    """
    AND of (approval-status:approved on whichever filter approval_filter_mode selects) and
    (time-entry-type:worked-time). See APPROVAL_FILTER_URI_BY_MODE for allowed mode values.
    """
    try:
        approval_filter_uri = APPROVAL_FILTER_URI_BY_MODE[approval_filter_mode]
    except KeyError:
        raise AirflowFailException(
            f"config.time_export_approval_filter_mode={approval_filter_mode!r} is not one of "
            f"{sorted(APPROVAL_FILTER_URI_BY_MODE)}"
        )
    return {
        "leftExpression": {
            "leftExpression": {
                "filterDefinitionUri": approval_filter_uri
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [
                        "urn:replicon:approval-status:approved"
                    ]
                }
            }
        },
        "operatorUri": "urn:replicon:filter-operator:and",
        "rightExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [
                        "urn:replicon:time-entry-type:worked-time"
                    ]
                }
            }
        }
    }


def create_time_export_payload(approval_filter_mode):
    """
    entry-date-range AND export-status:none AND (approval filter selected by approval_filter_mode,
    see _build_approval_and_entry_type_clause) AND time-entry-type:worked-time.
    export-status:none is the delta mechanism — entries already exported are excluded.
    Date range is computed in get_logging_details: start = first of current month (or previous
    month during day-1 grace period); end = today.
    """
    logging = rail.result("logging_details")
    start_date = logging["export_start_date_json"]
    end_date = logging["export_end_date_json"]
    return {
        "columnUris": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "dateRange": {
                                "startDate": start_date,
                                "endDate": end_date,
                                "relativeDateRangeUri": null,
                                "relativeDateRangeAsOfDate": null
                            }
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uris": [
                                "urn:replicon:time-data-item-time-data-export-status:none"
                            ]
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": _build_approval_and_entry_type_clause(approval_filter_mode)
        }
    }


def retrieve_export_uri(response):
    if response.get('error'):
        raise AirflowFailException(response)
    return response['timeDataExportUri']


def create_download_batch_payload(export_uri_task_id):
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [rail.result(export_uri_task_id)]
                }
            }
        },
        "fileFormatScriptUri": rail.result("get_time_download_script")
    }


def create_export_status_batch_payload(status, export_uri_task_id):
    """Used by cancel_time_export() — export_uri_task_id names the upstream task (e.g.
    'get_export_uri_failed' on the extraction-error path, or 'time_data_export.get_export_uri'
    on the validation/posting-failure path) whose result is this export batch's URI."""
    return {
        "target": {
            "uri": rail.result(export_uri_task_id),
            "name": null
        },
        "statusUri": f"urn:replicon:time-data-export-status:{status}"
    }


def build_bulk_get_users_request(logins):
    """
    Request body for ImportService1.svc/BulkGetUsers3, filtering by loginName (our export rows
    only carry Login_Name, not an employee id). parameterCorrelationId is set to the login itself
    so the response can be matched back to a login even if the API reorders/omits entries.
    """
    return {
        "users": [
            {
                "employeeId": null,
                "loginName": login,
                "parameterCorrelationId": login
            }
            for login in logins
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def build_primary_function_roles_request(user_uris):
    """
    Request body for ResourceService1.svc/BulkGetProjectRoleAssignmentScheduleForUsers.
    dateRange left fully null per the confirmed request/response example — "current effective"
    project role, not a role as-of any particular entry date.
    """
    return {
        "userUris": user_uris,
        "dateRange": {
            "startDate": null,
            "endDate": null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }
