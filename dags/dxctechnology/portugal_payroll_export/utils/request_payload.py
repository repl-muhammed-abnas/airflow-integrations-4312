from datetime import timedelta, datetime as dt
import pytz
import dateutil.relativedelta
import rail

def get_employeetype_child_hierarchy():
    return{
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:employee-type-group",
            "urn:replicon:location-list-column:full-path"
        ],
        "parentUri": rail.find_first_by_attr_and_get_attr(rail.result(
                                "get_enabled_employeetype_groups"), 'displayText', 'Contractor', 'uri')
    }

def get_employee_type_uris():
    contractor_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                                "get_enabled_employeetype_groups"), 'displayText', 'Contractor', 'uri')

    return list(map(lambda item: item['uri'], rail.result("get_child_hierarchy_data"))) + [contractor_uri]


def child_dag_conf(config):
    today = dt.today()
    start_date = (today - timedelta(days=today.weekday())) - timedelta(days=168)
    end_date = (today - timedelta(days=today.weekday())) + timedelta(days=6)

    utc_tz = dt.utcnow().replace(tzinfo=pytz.utc)
    hundred_days_back= utc_tz + dateutil.relativedelta.relativedelta(days=-100)

    return {
        "fileformat_uri": rail.find_first_by_attr_and_get_attr(rail.result(
                        "get_all_scripts"), 'displayText', config.fileformat_name, 'uri'),
        "startdate": start_date.strftime("%Y-%m-%d"),
        "startdateday": start_date.strftime("%d"),
        "startdatemonth": start_date.strftime("%m"),
        "startdateyear": start_date.strftime("%Y"),
        "enddate": end_date.strftime("%Y-%m-%d"),
        "enddateday": end_date.strftime("%d"),
        "enddatemonth": end_date.strftime("%m"),
        "enddateyear": end_date.strftime("%Y"),
        "division": config.company_code,
        "divisionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                        "get_enabled_companycodes"), 'displayText', config.company_code, 'uri'),
        "timenow": dt.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "rundateinYYYYMMDDformat": dt.now().strftime("%Y%m%d"),
        "runtimeinHHMMSSformat": dt.now().strftime("%H%M%S"),
        "employeetypeuris": get_employee_type_uris(),
        'payrollstartdateday': hundred_days_back.strftime("%d"),
        'payrollstartdatemonth': hundred_days_back.strftime("%m"),
        'payrollstartdateyear': hundred_days_back.strftime("%Y"),
    }

def get_all_required_pacodes(mapper):
    return "'"+"','".join(mapper)+"'"

def get_create_payrun_download_batch_payload(dag_run):
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": None,
                "filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": {
                    "uri": None,
                    "uris": [rail.result('get_payrun_batch_result')['payRunUri']],
                    "bool": None,
                    "date": None,
                    "money": None,
                    "number": None,
                    "text": None,
                    "time": None,
                    "calendarDayDurationValue": None,
                    "workdayDurationValue": None,
                    "dateRange": None,
                    "dateTimeUtc": None,
                    "dateTimeUtcRange": None
                },
                "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        },
        "fileFormatScriptUri": dag_run.conf['fileformat_uri']
    }

def get_create_payrun_batch_payload(dag_run):
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
                                    "year": dag_run.conf['startdateyear'],
                                    "month": dag_run.conf['startdatemonth'],
                                    "day": dag_run.conf['startdateday']
                                    },
                            "endDate": {
                                    "year": dag_run.conf['enddateyear'],
                                    "month": dag_run.conf['enddatemonth'],
                                    "day": dag_run.conf['enddateday']
                                },
                                    "relativeDateRangeUri": None,
                                    "relativeDateRangeAsOfDate": None
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
                    "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                    "value": {
                        "uris": [dag_run.conf['divisionuri']]
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
                        "uris": dag_run.conf['employeetypeuris']
                    }
                }
            }
        }
    }
}


def get_create_payroll_download_batch_payload(dag_run):
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
                                    "year": dag_run.conf['startdateyear'],
                                    "month": dag_run.conf['startdatemonth'],
                                    "day": dag_run.conf['startdateday']
                                    },
                            "endDate": {
                                    "year": dag_run.conf['enddateyear'],
                                    "month": dag_run.conf['enddatemonth'],
                                    "day": dag_run.conf['enddateday']
                                },
                        "relativeDateRangeUri": None,
                        "relativeDateRangeAsOfDate": None
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
                    "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                    "value": {
                        "uris": [dag_run.conf['divisionuri']]
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
                        "uris": dag_run.conf['employeetypeuris']
                        }
                    }
                }
                }
            },
            "fileFormatScriptUri": dag_run.conf['fileformat_uri']
        }

def get_all_past_time_export_data_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:pay-run-list-column:pay-run",
            "urn:replicon:pay-run-list-column:status",
            "urn:replicon:pay-run-list-column:creation-date"
        ],
        "sort": [
            {
                "columnUri": "urn:replicon:pay-run-list-column:creation-date",
                "isAscending": "false"
            }
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:pay-run-list-filter:cancelled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "bool": "false"
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:pay-run-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "PRT_Payrolldata"
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:pay-run-list-filter:creation-date-range"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "dateRange": {
                                "startDate": {
                                    "year": dag_run.conf['payrollstartdateyear'],
                                    "month": dag_run.conf['payrollstartdatemonth'],
                                    "day": dag_run.conf['payrollstartdateday']
                                },
                                "endDate": None
                            }
                        }
                    }
                }
            }
        }
    }
