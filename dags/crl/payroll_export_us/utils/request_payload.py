from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import pendulum
import rail
# pylint: disable=no-name-in-module
from crl.payroll_export_us.mapper.payroll_calendar_mapper import USA_PAYROLL_CALENDER_MAPPER_BIWEEKLY, USA_PAYROLL_CALENDER_MAPPER_WEEKLY


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


def get_export_start_date(end_date):
    return end_date + relativedelta(months=-6)


def get_export_end_date(time_zone, export_type):
    current_date = pendulum.now(time_zone).strftime("%d-%m-%Y")
    biweekly_calendar = rail.find_first_by_attr_and_get_attr(
        USA_PAYROLL_CALENDER_MAPPER_BIWEEKLY, "payroll_processing_date", current_date, "pay_period_end_date")
    weekly_calendar = rail.find_first_by_attr_and_get_attr(
        USA_PAYROLL_CALENDER_MAPPER_WEEKLY, "payroll_processing_date", current_date, "pay_period_end_date")
    return (dt.strptime(biweekly_calendar, "%d-%m-%Y") if biweekly_calendar else None) if (export_type == "BIWEEKLY") else (dt.strptime(weekly_calendar, "%d-%m-%Y") if weekly_calendar else None)


def get_create_payroll_download_batch_payload(time_zone, export_type):
    end_date = get_export_end_date(time_zone, export_type)
    start_date = get_export_start_date(end_date) if end_date else None
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
                                        "year": start_date.year if start_date else None,
                                        "month": start_date.month if start_date else None,
                                        "day": start_date.day if start_date else None
                                    },
                                    "endDate": {
                                        "year": end_date.year if end_date else None,
                                        "month": end_date.month if end_date else None,
                                        "day": end_date.day if end_date else None
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
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_filter_definition'),
                                                                                        "name", "US Pay Group", "uri")
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                # pylint: disable=line-too-long
                                "uris": [rail.find_first_by_attr_and_get_attr(rail.result('get_custom_field_dropdown'),
                                                 "displayText", data["displayText"] , "uri") for data in rail.result('get_custom_field_dropdown') if data["displayText"] != "NYW"] if (export_type == "BIWEEKLY") else [rail.find_first_by_attr_and_get_attr(rail.result('get_custom_field_dropdown'),
                                                 "displayText", data["displayText"] , "uri") for data in rail.result('get_custom_field_dropdown') if data["displayText"] == "NYW"]
                            }
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
                                "uris": rail.result('get_location_child_hierarchy_data')
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
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:user"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": rail.result("create_object_uris")
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
                                    "year": pendulum.now(time_zone).strftime("%Y"),
                                    "month": pendulum.now(time_zone).strftime("%m"),
                                    "day": pendulum.now(time_zone).strftime("%d"),
                                    "hour": pendulum.now(time_zone).strftime("%H"),
                                    "minute": pendulum.now(time_zone).strftime("%M"),
                                    "second": pendulum.now(time_zone).strftime("%S"),
                                    "millisecond": 0
                                }
                            }
                        }
                    }
                }
            }
        },
        "fileFormatScriptUri": rail.result("get_adp_payroll_script")
    }


def get_create_payrun_batch_payload(time_zone, export_type):
    end_date = get_export_end_date(time_zone, export_type)
    start_date = get_export_start_date(end_date)
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
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_filter_definition'),
                                                                                        "name", "US Pay Group", "uri")
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                # pylint: disable=line-too-long
                                "uris": [rail.find_first_by_attr_and_get_attr(rail.result('get_custom_field_dropdown'),
                                                 "displayText", data["displayText"] , "uri") for data in rail.result('get_custom_field_dropdown') if data["displayText"] != "NYW"] if (export_type == "BIWEEKLY") else [rail.find_first_by_attr_and_get_attr(rail.result('get_custom_field_dropdown'),
                                                 "displayText", data["displayText"] , "uri") for data in rail.result('get_custom_field_dropdown') if data["displayText"] == "NYW"]
                            }
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
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:user"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": rail.result("create_object_uris")
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
                                    "year": pendulum.now(time_zone).strftime("%Y"),
                                    "month": pendulum.now(time_zone).strftime("%m"),
                                    "day": pendulum.now(time_zone).strftime("%d"),
                                    "hour": pendulum.now(time_zone).strftime("%H"),
                                    "minute": pendulum.now(time_zone).strftime("%M"),
                                    "second": pendulum.now(time_zone).strftime("%S"),
                                    "millisecond": 0
                                }
                            }
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
                    "bool": "true"
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
