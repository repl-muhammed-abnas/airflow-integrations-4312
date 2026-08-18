from dateutil.relativedelta import relativedelta
import pendulum
from dateutil.parser import parse as date_parser
import json
import rail

null = None

# PTA Export: 6 months back to 5 weeks back
PTA_EXPORT_START_LOOKBACK_DAYS = 180  # ~6 months
PTA_EXPORT_END_LOOKBACK_DAYS = 35     # 5 weeks


def get_time_export_date_range(dag_run, config):
    """
    Get the date range for PTA time export
    PTA exports data from 6 months back to 5 weeks back
    """
    today = pendulum.now(config.time_zone)
    
    # Start date: 6 months (180 days) before today
    start_date = today - relativedelta(days=PTA_EXPORT_START_LOOKBACK_DAYS)
    
    # End date: 5 weeks (35 days) before today
    end_date = today - relativedelta(days=PTA_EXPORT_END_LOOKBACK_DAYS)
    
    return (start_date, end_date)


def get_create_time_data_export_batch_payload(dag_run, config):
    """
    Create the payload for time data export batch
    Filters:
    - Date range (6 months back to 5 weeks back for PTA)
    - Export status: none (not previously exported)
    - Time entry types: worked-time, time-off, allocation-time
    - Approval status: approved
    - Location: GBR (UK)
    """
    start_date, end_date = get_time_export_date_range(dag_run, config)

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
                                "filterDefinitionUri": "urn:replicon:time-data-export-filter:location"
                            },
                            "operatorUri": "urn:replicon:filter-operator:in",
                            "rightExpression": {
                                "value": {
                                    "uris": rail.result("get_allowed_location_uris")
                                }
                            }
                        }
                    }
                }
            }
        }
    }


def create_export_status_complete_batch_payload(export_uri):
    """
    Create payload to mark export status as complete
    """
    return {
        "target": {
            "uri": rail.result(export_uri),
            "name": null
        },
        "statusUri": "urn:replicon:time-data-export-status:complete"
    }


def form_download_parameters(group_id, file_script_uri):
    """
    Form download parameters for the export batch
    """
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
    """
    Get payload to cancel time off export
    """
    return {
        "target": {
            "uri": rail.result(group_id + ".get_export_uri")
        }
    }


def get_revert_draft_payload(group_id):
    """
    Get payload to revert export to draft status
    """
    return {
        "target": {
            "uri": rail.result(group_id + ".get_export_uri")
        }
    }


def get_allowed_location_uris_payload(export_locations):
    """
    Get payload to retrieve allowed location URIs
    """
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