from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import json
from pendulum import now
import rail

null = None

DAILY_TIME_EXPORT_RUN_PAST_LOOKBACK_PERIOD_IN_MONTHS = 1
DAILY_TIME_EXPORT_RUN_FUTURE_LOOKUP_PERIOD_IN_MONTHS = 4


def get_time_export_date_range():
    curr_date = now(
        tz=rail.result('get_logging_details')['timezone']).replace(day=1)

    start_date = curr_date - relativedelta(months=DAILY_TIME_EXPORT_RUN_PAST_LOOKBACK_PERIOD_IN_MONTHS)
    end_date = curr_date + relativedelta(months=DAILY_TIME_EXPORT_RUN_FUTURE_LOOKUP_PERIOD_IN_MONTHS)
    end_date = end_date - relativedelta(days=1)
    return (start_date, end_date)

def get_create_time_data_export_batch_payload(dag_run):
    start_date, end_date = get_time_export_date_range()

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
                                "endDate": {
                                    "year": end_date.year,
                                    "month": end_date.month,
                                    "day": end_date.day
                                },
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
                            ],
                        }
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
                                "urn:replicon:time-entry-type:time-off"
                            ]
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-approval-status"
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
            }
        }
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
            "uri": rail.result(group_id + ".get_export_uri"),
            "name": null
        }
    }


def get_revert_draft_payload(group_id):
    return {
        "target": {
            "uri": rail.result(group_id + ".get_export_uri"),
            "name": null
        }
    }


def convert_str_date_to_date(date_str):
    return dt.strptime(date_str, '%b %d, %Y')
