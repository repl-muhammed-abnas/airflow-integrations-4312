import pendulum
import rail

null = None


def get_costcenter_hierarchy():
    return {
        "page": "1",
        "pagesize": "10000000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:full-path",
            "urn:replicon:cost-center-list-column:cost-center"
        ],
        "sort": [],
        "filterExpression": null
    }


def get_location_uri_payload(location):
    return {
        "page": "1",
        "pageSize": "100",
        "textSearch": {
                "queryText": location,
                "searchInDisplayText": "false",
                "searchInName": "true",
                "searchInDescription": "false",
                "searchInCode": "false"
        }
    }


def get_parent_cost_center_payload(dag_run):
    return {
        "page": "1",
        "pageSize": "100",
        "textSearch": {
                "queryText": dag_run.conf["cost_center_name"],
                "searchInDisplayText": "false",
                "searchInName": "true",
                "searchInDescription": "false",
                "searchInCode": "false"
        }
    }


def get_create_payrun_batch_payload(dag_run, time_zone):
    current_time = pendulum.now(time_zone)
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
                                    "startDate": dag_run.conf["exportdetails"]["export_start_date_json"],
                                    "endDate": dag_run.conf["exportdetails"]["export_end_date_json"],
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
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:location"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in-hierarchy",
                        "rightExpression": {
                            "value": {
                                "uris": [dag_run.conf["uk_location_uri"]]
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:cost-center"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in-hierarchy",
                        "rightExpression": {
                            "value": {
                                "uris": [rail.result("get_cost_center_uri")]
                            }
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
                                "year": current_time.year,
                                "month": current_time.month,
                                "day": current_time.day,
                                "hour": current_time.hour,
                                "minute": current_time.minute,
                                "second": current_time.second,
                                "millisecond": 0
                            }
                        }
                    }
                }
            }
        }
    }


def get_create_payrun_download_batch_payload(dag_run):
    payrunuri = rail.result('get_payrun_batch_result')['payRunUri']
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [payrunuri],
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
                    "dateTimeUtcRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        },
        "fileFormatScriptUri": dag_run.conf["payroll_script_uri"]
    }


def get_payload():
    return {
        "target": {
            "uri": rail.result('get_payrun_batch_result')['payRunUri']
        }
    }
