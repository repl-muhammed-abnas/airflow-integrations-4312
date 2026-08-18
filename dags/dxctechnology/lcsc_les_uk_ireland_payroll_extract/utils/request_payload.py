from datetime import datetime as dt
import pendulum as pd
import rail
null = None

SQL_DATE_FORMAT = "%Y-%m-%d"
REPORT_DATE_FORMAT = "%d %B %Y"
MDY_DATE_FORMAT = "%m/%d/%Y"
YMD_DATE_FORMAT = "%Y%m%d"
HMS_DATE_FORMAT = "%H%M%S"

def get_conf():
    return rail.get_current_context()['dag_run'].conf

def get_sequence_no():
    return str(get_conf()['sequence_no'])

def get_logging_details(duration_days, time_zone):
    now = pd.now(time_zone)
    current_timesheet_start = now.start_of('week')
    current_timesheet_end = now.start_of('week').add(days=6)
    start_date = current_timesheet_start.subtract(days=duration_days)
    return {
        "current_date_mdy": now.strftime(MDY_DATE_FORMAT),
        "current_date_ymd": now.strftime(YMD_DATE_FORMAT),
        "current_time_hms": now.strftime(HMS_DATE_FORMAT),
        "start_date": {
            "year": start_date.year,
            "month": start_date.month,
            "day": start_date.day
        },
        "end_date": {
            "year": current_timesheet_end.year,
            "month": current_timesheet_end.month,
            "day": current_timesheet_end.day
        }
    }

def get_all_required_paycodes(lcsc_wage_codes_mapper, les_wage_codes_mapper):
    paycodes_mapper = lcsc_wage_codes_mapper if get_conf()['region'] == 'LCSC' else (
        les_wage_codes_mapper if get_conf()['region'] == 'LES' else null)
    paycodes = set(map(lambda paycodes_mapper: paycodes_mapper["wage_code"],
        filter(lambda paycodes_mapper: paycodes_mapper['location'] == get_conf()['location_name']
            and paycodes_mapper['wage_code'], paycodes_mapper)))
    return "'"+"','".join(paycodes)+"'"

def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)

def companycode_from_mapper(time, export, frequency, mapper):
    list_of_codes = list(filter(lambda item:
        item['time'] == time and item['export'] == export
            and item['frequency'] == frequency, mapper))
    return list_of_codes

def process_payrolldata_export_conf(item, time_zone, file_name_prefix):
    return {
        'file_format_name': item['file_format_name'],
        'file_format_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_specific_scripts"),
            "displayText", item['file_format_name'], "uri"),
        'location_code': item['location_code'],
        'location_name': item['location'],
        'location_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_specific_enabled_locations"),
            "displayText", item['location'], "uri"),
        'division_code': item['company_code'],
        'division_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_specific_enabled_divisions"),
            "displayText", item['company_code'], "uri"),
        'contractor_employee_type_uri': rail.result("get_specific_enabled_employee_type_groups"),
        'sequence_no': item['sequence_no'],
        'region': item['region'],
        'file_name': file_name_prefix +"_" + pd.now(time_zone).strftime("%Y%m%d%H%M%S") + "_" + item['location_code'] + "REPL_REPL" + item['sequence_no'] +"_DUT8G2I",
        'logging_details': rail.result("logging_details")
    }

def get_location_child_hierarchy_param():
    return{
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:full-path"
        ],
        "parentUri": get_conf()['location_uri']
    }

def get_employee_type_child_hierarchy_param():
    return{
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:employee-type-group",
            "urn:replicon:employee-type-group-list-column:full-path"
        ],
        "parentUri": get_conf()['contractor_employee_type_uri']
    }

def get_mapper_entry(wage_code, location_name, lcsc_wage_codes_mapper, les_wage_codes_mapper):
    """Look up mapper entry based on wage_code and location."""
    region = get_conf()['region']
    mapper = lcsc_wage_codes_mapper if region == 'LCSC' else (
        les_wage_codes_mapper if region == 'LES' else [])
    return next((e for e in mapper if e.get('location') == location_name and e.get('wage_code') == wage_code), null)

def get_compose_item_payroll_data_row(item, lcsc_wage_codes_mapper, les_wage_codes_mapper):
    personnelnumber = item['Actual_Employee_ID'] if item['Actual_Employee_ID'] else item['CLIID']
    location_name = get_conf()['location_name']
    wage_code = item['Pay_Code_Code']

    mapper_entry = get_mapper_entry(wage_code, location_name, lcsc_wage_codes_mapper, les_wage_codes_mapper)
    unit_code = mapper_entry.get('unit_code') if mapper_entry else null
    info_type = mapper_entry.get('info_type') if mapper_entry else null
    category = mapper_entry.get('category') if mapper_entry else null

    header = ("P" + info_type) if ((category == "Absence") and info_type) else "P2010"

    return [
        header,
        personnelnumber,
        get_conf()["location_code"],
        item['ORDNO'] if item['ORDNO'] else null,
        "INS",
        info_type,
        item['Pay_Code_Code'] if item['Pay_Code_Code'] else null,
        dt.strptime(item['BEGDA'], REPORT_DATE_FORMAT).strftime(YMD_DATE_FORMAT) if item['BEGDA'] else null,
        dt.strptime(item['ENDDA'], REPORT_DATE_FORMAT).strftime(YMD_DATE_FORMAT) if item['ENDDA'] else null,
        item['OBJPS'] if item['OBJPS'] else null,
        item['SPRPS'] if item['SPRPS'] else null,
        item['SEQNR'] if item['SEQNR'] else null,
        item['EXTRA'] if item['EXTRA'] else null,
        item['Pay_Code_Code'] if item['Pay_Code_Code'] else null,
        item['STDAZ'] if item['STDAZ'] else null,
        item['BEGUZ'] if item['BEGUZ'] else null,
        item['ENDUZ'] if item['ENDUZ'] else null,
        item['BETRG'] if item['BETRG'] else null,
        item['WAERS'] if item['WAERS'] else null,
        item['Pay_Code_Hours'],
        unit_code
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
        "fileFormatScriptUri": get_conf()['file_format_uri']
    }

def get_child_hierarchy_data(child_list, parent_uri):
    if child_list is not null:
        uri_list = child_list
        child_uris = [elem['uri'] for elem in uri_list]
        child_uris.append(parent_uri)
        return child_uris
    return null

def get_create_payrun_batch_payload():
    now = pd.now('Etc/UTC')
    child_location_uris = get_child_hierarchy_data(rail.result(
        'get_location_child_hierarchy_data'), get_conf()['location_uri'])
    employee_type_uris = get_child_hierarchy_data(rail.result('get_contractor_employee_type_child_hierarchy_data'
        ), get_conf()['contractor_employee_type_uri'])
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
                                    "startDate": get_conf()["logging_details"]["start_date"],
                                    "endDate": get_conf()["logging_details"]["end_date"]
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
                                "uris": [
                                    get_conf()['division_uri']
                                ]
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
                                "year": now.year,
                                "month": now.month,
                                "day": now.day,
                                "hour": now.hour,
                                "minute": now.minute,
                                "second": now.second,
                                "millisecond": 0
                            }
                        }
                    }
                }
            }
        }
    }
