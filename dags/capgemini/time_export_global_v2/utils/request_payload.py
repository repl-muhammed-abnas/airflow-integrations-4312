from datetime import datetime
from airflow.exceptions import AirflowFailException
import rail

null = None

def create_time_export_payload(dag_run):
    return {
        "columnUris": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "dateRange": {
                                    "startDate": dag_run.conf["logging_details"]["export_start_date_json"],
                                    "endDate": dag_run.conf["logging_details"]["export_end_date_json"],
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
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:timesheet-only-approval-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uris": [
                                "urn:replicon:approval-status:approved"
                            ]
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:location"
                    },
                    "operatorUri": "urn:replicon:filter-operator:not-in",
                    "rightExpression": {
                        "value": {
                            "uris": rail.result("get_restricted_location_uris")
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
                                "urn:replicon:time-entry-type:worked-time",
                                "urn:replicon:time-entry-type:time-off"
                            ]
                        }
                    }
                }
            }
        }
    }

def create_export_status_complete_batch_payload(export_uri):
    return {
        "target": {
            "uri": rail.result(export_uri),
            "name": null
        },
        "statusUri": "urn:replicon:time-data-export-status:complete"
    }

def create_export_status_batch_payload(status):
    return {
        "target": {
            "uri": rail.result("get_export_uri_failed"),
            "name": null
        },
        "statusUri": f"urn:replicon:time-data-export-status:{status}"
    }

def get_create_download_batch(export_uri):
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
                    "uris": [rail.result(export_uri)],
                },
            },
        },
        "fileFormatScriptUri": rail.result("get_time_download_script")
    }

def retrieve_export_uri(response):
    if response['error']:
        raise AirflowFailException(response)
    return response['timeDataExportUri']

def get_restricted_location_uris_payload(excepted_export_locations):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:location-list-column:location"
            ],
        "filterExpression": {
        "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon:location-list-filter:text"
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
            "text": excepted_export_locations,
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
        },
        "hierarchyListDataOptionUris": [
            "urn:replicon:hierarchy-list-data-option:include-descendant-rows"
            ]
        }

def get_specific_location_users_payload():
    return {
        "page": "1",
        "pagesize": "10",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:location"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:location"
                },
                "operatorUri": "urn:replicon:filter-operator:in-hierarchy",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": rail.result("get_specific_location_uri"),
                        "uris": [
                            null
                        ],
                        "bool": null,
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
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
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
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def get_bulk_user_data():
    return {
        "users": [
            {
                "uri": user_uri
            } for user_uri in rail.result("get_specific_location_users")
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_timesheet_details():
    timesheet_date = datetime.strptime(rail.result("get_start_date_var")["value"], "%Y-%m-%d")
    return {
        "userUri": rail.result("get_user_details"),
        "date": {
            "year": timesheet_date.year,
            "month": timesheet_date.month,
            "day": timesheet_date.day
        },
        "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
    }
