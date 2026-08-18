from datetime import datetime as dt
from dateutil.parser import parse as date_parser
import pendulum
import rail
# pylint: disable=no-name-in-module


null = None


def get_allowed_location_uris_payload():
    return {
        "page": "1",
        "pagesize": "10000000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:full-path"
        ],
        "parentUri": rail.result("get_all_enabled_locations")
    }



def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_compose_item_payroll_aus_data_row(item):
    if item["SUBTY"] == "TO":
        paycode_code = "2500"
    else:
        paycode_code = item["SUBTY"]
    return [
        item["RECTY"],
        item["CLIID"],
        item["INTCA"],
        item["ORDNO"],
        item["IOPER"],
        item["INFTY"],
        paycode_code,
        item['BEGDA'],
        item['ENDDA'],
        item["OBJPS"],
        item["SPRPS"],
        item["SEQNR"],
        item["EXTRA"],
        paycode_code,
        item["STDAZ"],
        item["BEGUZ"],
        item["ENDUZ"],
        item["BETRG"],
        item["WAERS"],
        item["ANZHL"].replace(",", ""),
        item["ZEINH"],
        item["VTKEN"],
        item["BWGRL"],
        item["AUFKZ"],
        item["ENDOF"],
        item["UFLD1"],
        item["UFLD2"],
        item["UFLD3"],
        item["KEYPR"],
        item["TRFGR"],
        item["TRFST"],
        item["PRAKN"],
        item["PRAKZ"],
        item["OTYPE"],
        item["PLANS"],
        item["VERSL"],
        item["EXBEL"],
        item["WTART"],
        item["TDLANGU"],
        item["TDSUBLA"],
        item["TDTYPE"]
    ]


def get_create_payrun_download_batch_payload():
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
        "fileFormatScriptUri": rail.result("get_adp_payroll_script")
    }


def get_export_end_date(dag_run):
    return (date_parser(dag_run.conf['webhook']['data']['start_date']), date_parser(dag_run.conf['webhook']['data']['end_date']))


def get_create_payroll_download_batch_payload(dag_run):
    start_date, end_date = get_export_end_date(dag_run)
    return {
        "columnUris": [],
        "sort": [],
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
                                        "year":  start_date.year,
                                        "month":  start_date.month,
                                        "day":  start_date.day
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
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:location"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": rail.result("get_location_child_hierarchy_data")
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:user"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": rail.result("create_object_uris")
                            }
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:employee-type-group"
                    },
                    "operatorUri": "urn:replicon:filter-operator:not-in",
                    "rightExpression": {
                        "value": {
                            "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_employee_type'),
                                                                        "displaytext", "Contingent Worker", "uri")
                        }
                    }
                }
            }
        },
        "fileFormatScriptUri": rail.result("get_adp_payroll_script")
    }


def get_create_payrun_batch_payload(dag_run):
    start_date, end_date = get_export_end_date(dag_run)
    return {
        "columnUris": [],
        "sort": [],
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
                                        "year":  start_date.year,
                                        "month":  start_date.month,
                                        "day":  start_date.day
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
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:location"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": rail.result("get_location_child_hierarchy_data")
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:user"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": rail.result("create_object_uris")
                            }
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:employee-type-group"
                    },
                    "operatorUri": "urn:replicon:filter-operator:not-in",
                    "rightExpression": {
                        "value": {
                            "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_employee_type'),
                                                                        "displaytext", "Contingent Worker", "uri")
                        }
                    }
                }
            }
        }
    }


def get_user_data():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:user"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "bool": "false"
                }
            }
        }
    }


def get_create_object_set(dag_run):
    return {
        "userUris": list(filter(None, list(dag_run.conf['uri'])))
    }


def get_payload():
    return {
        "target": {
            "uri": rail.result('get_payrun_batch_result')['payRunUri']
        }
    }

def get_sequence(calendra_mapper, time_zone):
    current_date = pendulum.now(time_zone).strftime("%d-%m-%Y")
    return "02" if bool(rail.find_first_by_attr_and_get_attr(
        calendra_mapper, "payroll_processing_date", current_date)) else "01"

def get_run_us_user_report_payload():
    get_specific_report_details = rail.result('get_user_report_details')

    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_formated_user_row(item):
    return {
        'username': item['User Name'],
        'location': item['Location (Current)'],
        'useruri': item['UserUri'],
        'userstartdate': dt.strptime(item['User Start Date'], "%b %d, %Y").strftime("%Y-%m-%d") if item['User Start Date'] else None,
        'userenddate': dt.strptime(item['User End Date'], "%b %d, %Y").strftime("%Y-%m-%d") if item['User End Date'] else None,
        'employeeid':item['Employee ID']
    }.values()
