from datetime import datetime as dt, timedelta
import rail
from dxctechnology.australia_termination_balance_v1.mapper.time_off_balance_mapper import Time_off_mappper, Disabled_user_timeoffs, Disabled_user_lsl_Timeoffs

null = None

def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

def get_all_required_pacodes(mapper):
    return "'"+"','".join(mapper)+"'"

def get_end_date_begin_of_week():
    return str(dt.utcnow().strftime("%Y-%m-%d"))

def get_utc_now_date_string(config):
    return dt.utcnow().strftime(config.date_time_format)

def get_export_start_date(date):
    cutoff_date = dt.strptime(date, "%Y-%m-%d")
    start_date = (dt.utcnow() - timedelta(days=dt.utcnow().weekday())) - timedelta(days=84)
    if start_date < cutoff_date and (rail.get_company_key()).lower() == "dxctechnology":
        return cutoff_date
    return start_date

def get_terminated_users(cutoff_date):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:division",
            "urn:replicon:user-list-column:end-date",
            "urn:replicon:user-list-column:employee-id"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:user-list-filter:end-date-range"
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
                            "year": get_export_start_date(cutoff_date).strftime("%Y"),
                            "month": get_export_start_date(cutoff_date).strftime("%m"),
                            "day": get_export_start_date(cutoff_date).strftime("%d")
                        },
                        "endDate": {
                            "year": dt.utcnow().strftime("%Y"),
                            "month": dt.utcnow().strftime("%m"),
                            "day": dt.utcnow().strftime("%d")
                        },
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def get_child_dagrun_conf(region):
    time_off_list= Disabled_user_timeoffs
    time_off_type_uris=[]

    for time_off_type in time_off_list:
        time_off_type_uris.append(rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeOffTypes"
                                                    ), "displayText", time_off_type, "uri"))
    return {
        "fileformaturi": rail.result('get_required_data')['script_uri'],
        "division": rail.result('get_required_data')['division'],
        "divisionuri": rail.result('get_required_data')['divisionuri'],
        "timenow": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        "rundateinYYYYMMDDformat": dt.utcnow().strftime("%Y%m%d"),
        "runtimeinHHMMSSformat": dt.utcnow().strftime("%H%M%S"),
        "today": rail.get_replicon_date(dt.utcnow()),
        "useruri": list(set(map(lambda x: x['useruri'], rail.load_all_records(rail.result('query_list_terminated_users'))))),
        "userids": list(set(map(lambda x: x['userid'], rail.load_all_records(rail.result('query_list_terminated_users'))))),
        'timeofftype_uris': time_off_type_uris,
        "division_name": region,
        "sequence_no": '01',
        "file_diff": "GS" if region == "GSAP" else "CP"
    }

def get_regular_child_dagrun_conf(dag_run):
    return {
        "division_name": 'GSAP' if dag_run.conf['division_name'] == 'GSAP' else 'AUES',
        'location_name': "Australia",
        "sequence_no": '02',
        "file_diff": dag_run.conf['file_diff']
    }

def get_run_termination_balance_report_payload(dag_run):
    get_specific_report_details = rail.result(
        'get_termination_balance_report_details')
    filter_values = []
    user_uris = [x['useruri'] for x in rail.load_all_records(rail.result("query_users_for_report"))]
    for item in user_uris:
        filter_values.append({
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_termination_balance_report_details'
                            )['filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri'),
            "value": item.split(':')[-1] if item else None
        })

    for timeofftype in dag_run.conf['timeofftype_uris']:
        if timeofftype:
            filter_values.append({
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_termination_balance_report_details'
                                )['filterConfiguration']['enabledFilters'], 'displayText', 'TimeOffTypeFilter', 'uri'),
                "value": timeofftype.split(':')[-1] if timeofftype else None
            })

    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()



def get_termination_balance_us_data_row(items):
    get_user_data_in_query = list(filter(lambda item: item['emp_id']== items['Employee_ID'], map(lambda item:{
        'emp_id': item['Employee_ID'],
        'timeoff_balance': item['Time_Off_Balance']
    },rail.load_all_records(rail.result("query_active_user_balance_data")))))

    time_off_balance = list(filter(lambda x:  x['Time off type'] == items['Time_Off_Type'], Time_off_mappper))

    personnelnumber = ""
    if items['Actual_Employee_ID']:
        personnelnumber = items['Actual_Employee_ID']
    else:
        personnelnumber = items['Employee_ID']

    timeoff_balance = items['Time_Off_Balance']
    if get_user_data_in_query and items['Time_Off_Type'] in Disabled_user_lsl_Timeoffs:
        timeoff_balance = str(round(float(get_user_data_in_query[0]['timeoff_balance'].replace(",","")) + float(items['Time_Off_Balance'].replace(",","")),2))

    return ["P2006",
            personnelnumber,
            "AU",
            "",
            "INS",
            "2006",
            time_off_balance[0]['Quote Type'],
            dt.strptime(items['User_End_Date'], "%d %B %Y").strftime("%Y%m%d") if items['User_End_Date'] else "",
            "99991231",
            "",
            "",
            "",
            "",
            "",
            "",
            time_off_balance[0]['Quote Type'],
            timeoff_balance.split("- ")[-1].replace(",",""),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
            ]


def is_upload_data_to_sftp_failed():
    if get_task_state('upload_export_data_to_sftp') == 'failed':
        return True
    return False


def is_upload_log_to_sftp_failed():
    if get_task_state('upload_log_data_to_sftp') == 'failed':
        return True
    return False


def remove_delimiter():
    return is_upload_data_to_sftp_failed()

def get_create_payroll_download_batch_payload(cutoff_date):
    return {
                "columnUris": [],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:entry-date-range"
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
                                        "year": get_export_start_date(cutoff_date).strftime("%Y"),
                                        "month": get_export_start_date(cutoff_date).strftime("%m"),
                                        "day": get_export_start_date(cutoff_date).strftime("%d")
                                    },
                                    "endDate": {
                                        "year": dt.utcnow().strftime("%Y"),
                                        "month": dt.utcnow().strftime("%m"),
                                        "day": dt.utcnow().strftime("%d")
                                    },
                                    "relativeDateRangeUri": null,
                                    "relativeDateRangeAsOfDate": null
                                },
                                "dateTimeUtc": null,
                                "dateTimeUtcRange": null
                            },
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
                                "filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run-status"
                            },
                            "operatorUri": "urn:replicon:filter-operator:in",
                            "rightExpression": {
                                "leftExpression": null,
                                "operatorUri": null,
                                "rightExpression": null,
                                "value": {
                                    "uri": null,
                                    "uris": [
                                        "urn:replicon:payable-time-pay-run-status:none"
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
                                    "dateTimeUtcRange": null
                                },
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
                                    "filterDefinitionUri": "urn:replicon:pay-run-filter:payable-time-approval-status"
                                },
                                "operatorUri": "urn:replicon:filter-operator:in",
                                "rightExpression": {
                                    "leftExpression": null,
                                    "operatorUri": null,
                                    "rightExpression": null,
                                    "value": {
                                        "uri": null,
                                        "uris": [
                                            "urn:replicon:payable-time-approval-status:approved"
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
                                        "dateTimeUtcRange": null
                                    },
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
                                        "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
                                    },
                                    "operatorUri": "urn:replicon:filter-operator:in",
                                    "rightExpression": {
                                        "leftExpression": null,
                                        "operatorUri": null,
                                        "rightExpression": null,
                                        "value": {
                                            "uri": null,
                                            "uris": rail.get_dag_run_conf()['divisionuri'],
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
                                "operatorUri": "urn:replicon:filter-operator:and",
                                "rightExpression": {
                                    "leftExpression": {
                                        "leftExpression": null,
                                        "operatorUri": null,
                                        "rightExpression": null,
                                        "value": null,
                                        "filterDefinitionUri": "urn:replicon:pay-run-filter:user"
                                    },
                                    "operatorUri": "urn:replicon:filter-operator:in",
                                    "rightExpression": {
                                        "leftExpression": null,
                                        "operatorUri": null,
                                        "rightExpression": null,
                                        "value": {
                                            "uri": null,
                                            "uris": [rail.result('create_object_set')],
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
                                "value": null,
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
                },
                "fileFormatScriptUri": rail.get_dag_run_conf()['fileformaturi']
            }

def get_create_payrun_batch_payload(cutoff_date):
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:pay-run-filter:entry-date-range"
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
                            "year": get_export_start_date(cutoff_date).strftime("%Y"),
                            "month": get_export_start_date(cutoff_date).strftime("%m"),
                            "day": get_export_start_date(cutoff_date).strftime("%d")
                        },
                        "endDate": {
                            "year": dt.utcnow().strftime("%Y"),
                            "month": dt.utcnow().strftime("%m"),
                            "day": dt.utcnow().strftime("%d")
                        },
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                        "dateTimeUtc": null,
                        "dateTimeUtcRange": null
                    },
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
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [
                                "urn:replicon:payable-time-pay-run-status:none"
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
                            "dateTimeUtcRange": null
                        },
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
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:payable-time-approval-status"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": [
                                    "urn:replicon:payable-time-approval-status:approved"
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
                                "dateTimeUtcRange": null
                            },
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
                                "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
                            },
                            "operatorUri": "urn:replicon:filter-operator:in",
                            "rightExpression": {
                                "leftExpression": null,
                                "operatorUri": null,
                                "rightExpression": null,
                                "value": {
                                    "uri": null,
                                    "uris": rail.get_dag_run_conf()['divisionuri'],
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
                        "operatorUri": "urn:replicon:filter-operator:and",
                        "rightExpression": {
                            "leftExpression": {
                                "leftExpression": null,
                                "operatorUri": null,
                                "rightExpression": null,
                                "value": null,
                                "filterDefinitionUri": "urn:replicon:pay-run-filter:user"
                            },
                            "operatorUri": "urn:replicon:filter-operator:in",
                            "rightExpression": {
                                "leftExpression": null,
                                "operatorUri": null,
                                "rightExpression": null,
                                "value": {
                                    "uri": null,
                                    "uris": [rail.result('create_object_set')],
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
                        "value": null,
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
        },
        "fileFormatScriptUri": rail.get_dag_run_conf()['fileformaturi']
    }

def get_create_payrun_download_batch_payload(dag_run):
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
                            "uris": [rail.result('get_payrun_batch_result')['payRunUri']],
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
                "fileFormatScriptUri": dag_run.conf['fileformaturi']
            }

def get_run_us_user_report_payload(dag_run):
    get_specific_report_details = rail.result('get_user_report_details')
    filter_values = []

    for item in dag_run.conf['userids']:
        filter_values.append({
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_user_report_details'
                            )['filterConfiguration']['enabledFilters'], 'displayText', 'CurrentDivisionFilter', 'uri'),
            "value": item,
        })

    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def process_absence_taken_conf():
    dag_run_conf = get_dag_run_conf()
    return {
        'location_name': "Australia",
        'division_name': dag_run_conf['division_name'],
        "sequence_no": '03',
        "file_diff": dag_run_conf['file_diff']
    }

def get_compose_item_payroll_aus_data_row(items):
    personnelnumber = ""
    if items['Actual_Employee_ID']:
        personnelnumber = items['Actual_Employee_ID']
    else:
        personnelnumber = items['CLIID']

    return ["P2010",
            personnelnumber,
            "AU",
            0,
            "INS",
            "2010",
            items['Pay_Code_Code'],
            dt.strptime(items['BEGDA'], "%d %B %Y").strftime(
                "%Y%m%d") if items['BEGDA'] else None,
            dt.strptime(items['ENDDA'], "%d %B %Y").strftime(
                "%Y%m%d") if items['ENDDA'] else None,
            "",
            "",
            "",
            "",
            items['Pay_Code_Code'],
            "",
            "",
            "",
            "",
            "",
            items['Pay_Code_Hours'].replace(",",""),
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
            "",
            ""
        ]
