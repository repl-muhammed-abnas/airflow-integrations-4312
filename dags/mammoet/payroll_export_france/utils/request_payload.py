from datetime import datetime, timedelta
import rail

null = None
DAILY_TIME_EXPORT_RUN_LOOKBACK_PERIOD_IN_WEEKS = 12


def get_time_export_date_range(dag_run):
    end_date = datetime.now() - timedelta(days=1)

    if dag_run.conf['payroll_export_run_type'] == 'monthly':
        return (datetime(end_date.year, 1, 1), end_date)

    start_date = end_date - \
        timedelta(weeks=DAILY_TIME_EXPORT_RUN_LOOKBACK_PERIOD_IN_WEEKS)
    return (start_date, end_date)


def get_create_payroll_batch_payload(dag_run):
    start_date, end_date = get_time_export_date_range(dag_run)
    return {
        "columnUris": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:entry-date-range"
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
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run-status"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": [
                                    "urn:replicon:payable-time-pay-run-status:none"
                                ]
                            }
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:payable-time-approval-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uris": [
                                "urn:replicon:payable-time-approval-status:approved"
                            ]
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:location"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uris": [dag_run.conf['payroll_location_uri']]+list(
                                map(lambda location: location['uri'], rail.result("get_all_location_for_parent")))
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:as-of-date-time-utc"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "dateTimeUtc": {
                                "year": end_date.year,
                                "month": end_date.month,
                                "day": end_date.day,
                                "hour": end_date.hour,
                                "minute": end_date.minute,
                                "second": end_date.second,
                                "millisecond": "0"
                            }
                        }
                    }
                }
            }
        }
    }


def form_download_parameters(group_id, file_script_uri):
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [rail.result(group_id + ".get_export_uri")],
                },
            },
        },
        "fileFormatScriptUri": rail.result(file_script_uri)
    }


def get_revert_draft_or_cancel_payroll_export_payload(group_id):
    return {
        "target": {
            "uri": rail.result(group_id + ".get_export_uri"),
            "name": null
        }
    }


def get_all_locations_for_parent(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:location-list-column:location"
        ],
        "parentUri": dag_run.conf['payroll_location_uri']
    }


def get_payroll_location_uri_payload(config):
    return {
        "page": "1",
        "pageSize": "10000",
        "textSearch": {
            "queryText": config.PAYROLL_LOCATION_NAME,
            "searchInDisplayText": "1",
            "searchInName": "0",
            "searchInDescription": "0",
            "searchInCode": "0"
        }
    }
