from datetime import timedelta, datetime as dt
import json
from pendulum import now, datetime
import rail

null = None

DAILY_TIME_EXPORT_RUN_LOOKBACK_PERIOD_IN_WEEKS = 12


def get_time_export_date_range(dag_run):
    end_date = now(
        tz=rail.result('get_logging_details')['timezone'])

    start_date = end_date - \
        timedelta(weeks=DAILY_TIME_EXPORT_RUN_LOOKBACK_PERIOD_IN_WEEKS)
    return (start_date, end_date)


def get_create_time_data_export_batch_payload(dag_run):
    start_date, end_date = get_time_export_date_range(dag_run)

    return {
        "columnUris": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
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
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
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
                            },
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
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [
                                "urn:replicon:time-data-item-time-data-export-status:none"
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
                "value": null,
                "filterDefinitionUri": null
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [
                                "urn:replicon:time-entry-type:worked-time"
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
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-approval-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [
                                "urn:replicon:approval-status:approved"
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
                "value": null,
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
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


def get_ts_min_max_dates():
    ts_data = [timesheet_period['timesheet_period'] for timesheet_period in rail.load_all_records(rail.result('query_unique_timesheets'))]
    min_date, max_date = convert_str_date_to_date(ts_data[0].split(' - ')[0]), convert_str_date_to_date(ts_data[0].split(' - ')[1])
    for date_range in ts_data:
        _min_date, _max_date = convert_str_date_to_date(date_range.split(' - ')[0]), convert_str_date_to_date(date_range.split(' - ')[1])
        min_date = min(min_date, _min_date)
        max_date = max(max_date, _max_date)
    return (min_date, max_date)


def get_all_timesheet_for_user():
    min_date , max_date = get_ts_min_max_dates()
    return {
        "page": "1",
        "pagesize": "100000",
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
                "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-owner"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                "uri": rail.result('get_user_details')['userDetails']['uri']
                }
            }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-period-date-range"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                "dateRange": {
                    "startDate": {
                        "year": min_date.year,
                        "month": min_date.month,
                        "day": min_date.day
                    },
                    "endDate": {
                        "year":max_date.year,
                        "month": max_date.month,
                        "day": max_date.day
                    }
                }
                }
            }
            }
        }
    }
