from datetime import datetime as dt, timedelta
import json
from dateutil.parser import parse as date_parser
import rail

null = None

DAILY_TIME_EXPORT_RUN_LOOKBACK_PERIOD_IN_WEEKS = 12

def get_time_export_date_range(dag_run):
    if dag_run.conf['start_date'] and dag_run.conf['end_date']:
        return (date_parser(dag_run.conf['start_date']), date_parser(dag_run.conf['end_date']))
    return (dt.now() - timedelta(days=35), dt.now())

def get_create_time_data_export_batch_payload(dag_run):
    start_date, end_date = get_time_export_date_range(dag_run)

    return {
        "columnUris": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": {
                                "year": start_date.year,
                                "month": start_date.month,
                                "day": start_date.day
                            },
                            "endDate": {
                                "year": end_date.year,
                                "month": end_date.month,
                                "day": end_date.day
                            }
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
            "leftExpression": {
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
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
                    },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uris": [
                                "urn:replicon:time-entry-type:worked-time",
                                "urn:replicon:time-entry-type:time-off",
                                "urn:replicon:time-entry-type:allocation-time"
                        ]
                    }
                }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:approval-status"
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
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:location"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                    "value": {
                        "uris": rail.result("get_allowed_location_uris")
                        }
                    }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:service-center"
                    },
                    "operatorUri": "urn:replicon:filter-operator:not-in",
                    "rightExpression": {
                        "value": {
                        "uris": [rail.result("get_company_codes_to_ignore")]
                        }
                    }
                    }
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


def form_download_parameters(group_id, file_script_uri):
    return json.dumps({
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [rail.result(group_id + ".get_export_uri")],
                },
            },
        },
        "fileFormatScriptUri": rail.result(file_script_uri)
    })


def get_cancel_timeoff_export_payload(group_id):
    return {
        "target": {
            "uri": rail.result(group_id + ".get_export_uri")
        }
    }


def get_revert_draft_payload(group_id):
    return {
        "target": {
            "uri": rail.result(group_id + ".get_export_uri")
        }
    }

def get_allowed_location_uris_payload(export_locations):
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:full-path"
            ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:location-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": export_locations
                }
            }
        },
        "hierarchyListDataOptionUris": [
            "urn:replicon:hierarchy-list-data-option:include-descendant-rows"
        ]
    }
