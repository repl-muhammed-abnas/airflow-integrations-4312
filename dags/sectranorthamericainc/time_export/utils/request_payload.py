from datetime import datetime, timedelta
import json
from pendulum import now
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


def get_time_export_batch_creation_payload():
    end_date = now()
    start_date = end_date - \
        timedelta(weeks=DAILY_TIME_EXPORT_RUN_LOOKBACK_PERIOD_IN_WEEKS)
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
                                "startDate": {
                                    "year": start_date.year,
                                    "month": start_date.month,
                                    "day": start_date.day
                                },
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


def get_report_generation_params():
    approval_date_filter_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_timesheet_day_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'ApprovalDateFilter', 'uri')
    todays_date = datetime.utcnow()
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
                        "value": (todays_date - timedelta(days=1)).strftime("%Y/%m/%d")
                    },
                    {
                        "reportFilterUri": approval_date_filter_uri,
                        "value": todays_date.strftime("%Y/%m/%d")
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                "reportUri": rail.result('get_timesheet_day_report_details')['uri']
            }
        ]
    }
