import json
import rail

null = None

DAILY_TIME_EXPORT_RUN_LOOKBACK_PERIOD_IN_WEEKS = 12


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


def get_revert_draft_cancel_time_export_payload(group_id=None):
    return {
        "target": {
            "uri": rail.result(group_id + ".get_export_uri"),
            "name": null
        }
    }


def get_time_export_batch_creation_payload(dag_run):
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
                                "startDate": dag_run.conf['timeexport']['start_date'],
                                "endDate": null,
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
        }
    }


def get_report_generation_params(dag_run):
    approval_date_filter_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_timesheet_day_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'ApprovalDateFilter', 'uri')
    
    return {
        "reportParameters": [
            {
                "filterValues": [
                    {
                        "reportFilterUri": approval_date_filter_uri,
                        "value": null
                    },
                    {
                        "reportFilterUri": approval_date_filter_uri,
                        "value": dag_run.conf['report']['start_date']
                    },
                    {
                        "reportFilterUri": approval_date_filter_uri,
                        "value": dag_run.conf['report']['end_date']
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                "reportUri": rail.result('get_timesheet_day_report_details')['uri']
            }
        ]
    }
