from datetime import datetime as dt, timedelta
import json
import pendulum as pd
import rail
from dxctechnology.csc_payroll_extract_v3.mapper.company_code_mapper import COMPANY_CODE_MAP
from dxctechnology.csc_payroll_extract_v3.mapper.attendancetypemapper import ATTENDANCE_TYPE_MAP


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def companycode_from_mapper(time, export, frequency):

    list_of_codes = list(filter(lambda item: item['Time'] == time and item['Export']
                                == export and item['Frequency'] == frequency, COMPANY_CODE_MAP))

    return list_of_codes


def get_start_date():
    begin_of_week = dt.utcnow() - timedelta(days=dt.utcnow().weekday())
    return json.dumps({"year": (begin_of_week - timedelta(days=84)).strftime("%Y"),
                       "month": (begin_of_week - timedelta(days=84)).strftime("%m"),
                       "day": (begin_of_week - timedelta(days=84)).strftime("%d")
                       })


def get_end_date():
    return json.dumps({"year": dt.utcnow().strftime("%Y"),
                       "month": dt.utcnow().strftime("%m"),
                       "day": dt.utcnow().strftime("%d")
                       })


def process_payrolldata_export_conf(file_format):
    return {
        'file_format_name': file_format,
        'file_format_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_scripts"
                                            ), "displayText", file_format, "uri")) if rail.result("get_all_scripts") else None,
        'location_name': "Canada",
        'division_uri': [(rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_divisions"
                                            ), "displayText", "1102", "uri")),(rail.find_first_by_attr_and_get_attr(
                                                rail.result("get_all_enabled_divisions"
                                            ), "displayText", "1219", "uri")),(rail.find_first_by_attr_and_get_attr(
                                                rail.result("get_all_enabled_divisions"
                                            ), "displayText", "1103", "uri")),(rail.find_first_by_attr_and_get_attr(
                                                rail.result("get_all_enabled_divisions"
                                            ), "displayText", "1105", "uri"))],
        'location_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_locations"
                    ), "displayText", "Canada", "uri")),
        'contractor_employee_type_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_employee_type_groups"
                                            ), "displayText", 'Contractor', "uri")) if rail.result("get_all_enabled_employee_type_groups") else None
    }


def get_location_child_hierarchy_param():
    conf = rail.get_current_context()['dag_run'].conf
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:full-path"
        ],
        "parentUri": conf['location_uri']
    }


def get_employee_type_child_hierarchy_param():
    conf = rail.get_current_context()['dag_run'].conf
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:employee-type-group",
            "urn:replicon:employee-type-group-list-column:full-path"
        ],
        "parentUri": conf['contractor_employee_type_uri']
    }


def get_remove_delimeter():
    users_data = get_data_from_document(
        rail.result('compose_item_payroll_csv_file'))
    each_item = "Personal NumberFillerSSNFillerDateTime TypeHoursLCD OrgFiller\n"
    for item in users_data:
        each_item += "".join(list(dict(item).values()))
        each_item += "\n"
    return each_item


def get_compose_item_payroll_data_row(items):
    final_time_type = items['Time_Type']
    personnelnumber = ""
    check_start_date = bool(dt.strptime(items['International_Assignee_start_date'], "%d %B %Y") < dt.strptime(
        items['Date'], "%m/%d/%Y")) if items['International_Assignee_start_date'] else False
    if items['Actual_Employee_ID']:
        personnelnumber = items['Actual_Employee_ID']
    else:
        if items['International_Assignee'] == "1" and items['International_Assignee_start_date'] and check_start_date:
            personnelnumber = items['Ia_perner_ID'].rjust(8, "0")
        else:
            personnelnumber = items['Personnel_Number'].rjust(8, "0")

    time_type = list(filter(
        lambda x: x['AAType'] == items['Time_Type'], ATTENDANCE_TYPE_MAP))
    if time_type:
        final_time_type = time_type[0]['TypeToExport'] if time_type[0]['TypeToExport'] else items['Pay_Code_Code']
    return ["P2010",
            personnelnumber,
            "CA",
            "",
           "INS",
            "2010",
            final_time_type,
            dt.strptime(items['Date'], "%m/%d/%Y").strftime("%Y%m%d"),
            dt.strptime(items['Date'], "%m/%d/%Y").strftime("%Y%m%d"),
            "",
            "",
            "",
            "",
            final_time_type,
            "",
            "",
            "",
            "",
            "",
            items['Hours'],
            "001",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
            ]


def get_create_payrun_download_batch_payload():
    conf = rail.get_current_context()['dag_run'].conf
    null = None
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
        "fileFormatScriptUri": conf['file_format_uri']
    }


def get_child_hierarchy_data(child_list, parent_uri):
    if child_list is not None:
        uri_list = child_list
        child_uris = [elem['uri'] for elem in uri_list]
        child_uris.append(parent_uri)
        return child_uris
    return None


def get_create_payroll_download_batch_payload(duration_days):
    conf = rail.get_current_context()['dag_run'].conf
    begin_of_week = dt.utcnow() - timedelta(days=dt.utcnow().weekday())
    null = None
    child_location_uris = get_child_hierarchy_data(rail.result(
        'get_location_child_hierarchy_data'), conf['location_uri'])
    employee_type_uris = get_child_hierarchy_data(rail.result('get_contractor_employee_type_child_hierarchy_data'
                                                              ), conf['contractor_employee_type_uri'])
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
                                    "startDate": {"year": (begin_of_week - timedelta(days=duration_days)).strftime("%Y"),
                                                  "month": (begin_of_week - timedelta(days=duration_days)).strftime("%m"),
                                                  "day": (begin_of_week - timedelta(days=duration_days)).strftime("%d")
                                                  },
                                    "endDate": {"year": (pd.now('UTC').end_of('week') - timedelta(days=2)).strftime("%Y"),
                                                "month": (pd.now('UTC').end_of('week') - timedelta(days=2)).strftime("%m"),
                                                "day": (pd.now('UTC').end_of('week') - timedelta(days=2)).strftime("%d")
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
                                "urn:replicon:payable-time-approval-status:approved", "urn:replicon:payable-time-approval-status:submitted"
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
                                "uris": child_location_uris
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": conf['division_uri']
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
                            "uris": employee_type_uris
                        }
                    }
                }
            }
        },
        "fileFormatScriptUri": conf['file_format_uri']
    }


def get_create_payrun_batch_payload(duration_days):
    conf = rail.get_current_context()['dag_run'].conf
    begin_of_week = dt.utcnow() - timedelta(days=dt.utcnow().weekday())
    child_location_uris = get_child_hierarchy_data(rail.result(
        'get_location_child_hierarchy_data'), conf['location_uri'])
    employee_type_uris = get_child_hierarchy_data(rail.result('get_contractor_employee_type_child_hierarchy_data'
                                                              ), conf['contractor_employee_type_uri'])
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
                                    "startDate": {"year": (begin_of_week - timedelta(days=duration_days)).strftime("%Y"),
                                                  "month": (begin_of_week - timedelta(days=duration_days)).strftime("%m"),
                                                  "day": (begin_of_week - timedelta(days=duration_days)).strftime("%d")
                                                  },
                                    "endDate": {"year": (pd.now('UTC').end_of('week') - timedelta(days=2)).strftime("%Y"),
                                                "month": (pd.now('UTC').end_of('week') - timedelta(days=2)).strftime("%m"),
                                                "day": (pd.now('UTC').end_of('week') - timedelta(days=2)).strftime("%d")
                                    }
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
                                    "urn:replicon:payable-time-approval-status:approved", "urn:replicon:payable-time-approval-status:submitted"
                                ]
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:location"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": child_location_uris
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
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": conf['division_uri']
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
                                "uris": employee_type_uris
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
                                "year": (pd.now('UTC')).strftime("%Y"),
                                "month": (pd.now('UTC')).strftime("%m"),
                                "day": (pd.now('UTC')).strftime("%d"),
                                "hour": (pd.now('UTC')).strftime("%H"),
                                "minute": (pd.now('UTC')).strftime("%M"),
                                "second": (pd.now('UTC')).strftime("%S"),
                                "millisecond": "0"
                            }
                        }
                    }
                }
            }
        }
    }

def get_sequence_no():
    conf = rail.get_current_context()['dag_run'].conf
    return str(conf['sequence_no'])
