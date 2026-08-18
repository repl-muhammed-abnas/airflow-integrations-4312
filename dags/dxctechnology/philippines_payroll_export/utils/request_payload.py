from datetime import datetime as dt
import rail


DATE_FORMAT = "%Y-%m-%d"


def get_employeetype_child_hierarchy():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:employee-type-group",
            "urn:replicon:location-list-column:full-path"
        ],
        "parentUri": rail.find_first_by_attr_and_get_attr(rail.result(
            "get_enabled_employeetype_groups"), 'displayText', 'Contractor', 'uri')
    }


def get_all_required_pacodes(mapper):
    return "'"+"','".join(mapper)+"'"


def get_create_payrun_download_batch_payload(dag_run):
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
                    "uris": [rail.result('get_payrun_batch_result')['payRunUri']]
                }
            }
        },
        "fileFormatScriptUri": rail.result("current_export_details")['fileformat_uri']
    }


def get_create_payrun_batch_payload(dag_run):
    current_time = dt.now()
    return {
        "columnUris": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:pay-run-filter:entry-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": rail.parse_date(rail.result("current_export_details")['startdate'], DATE_FORMAT),
                            "endDate": rail.parse_date(rail.result("current_export_details")['enddate'], DATE_FORMAT)
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
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
                            "leftExpression": {
                                "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
                            },
                            "operatorUri": "urn:replicon:filter-operator:in",
                            "rightExpression": {
                                "value": {
                                    "uris": rail.result("current_export_details")['divisionuri']
                                }
                            }
                        },
                        "operatorUri": "urn:replicon:filter-operator:and",
                        "rightExpression": {
                            "leftExpression": {
                                "leftExpression": {
                                    "filterDefinitionUri": "urn:replicon:pay-run-filter:employee-type-group"
                                },
                                "operatorUri": "urn:replicon:filter-operator:not-in",
                                "rightExpression": {
                                    "value": {
                                        "uris": rail.result("current_export_details")['contractor_uris']
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
            }
        }
    }


def get_create_payroll_download_batch_payload():
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
                                    "startDate": rail.parse_date(rail.result("current_export_details")['startdate'], DATE_FORMAT),
                                    "endDate": rail.parse_date(rail.result("current_export_details")['enddate'], DATE_FORMAT)
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
                            "uris": rail.result("current_export_details")['divisionuri']
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
                            "uris": rail.result("current_export_details")['contractor_uris']
                        }
                    }
                }
            }
        },
        "fileFormatScriptUri": rail.result("current_export_details")['fileformat_uri']
    }


def child_dag_conf():
    return {
        'regular_export_file_name': rail.result("current_export_details")['regular_filename'],
        'timeoff_export_file_name': rail.result("current_export_details")['timeoff_filename'],
        "process_started": rail.result("current_export_details")['process_started'],
        'log': rail.result("create_log")
    }
