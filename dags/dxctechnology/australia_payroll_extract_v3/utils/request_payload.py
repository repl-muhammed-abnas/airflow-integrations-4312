import calendar
from datetime import datetime as dt, timedelta, date
import json
import rail
from dxctechnology.australia_payroll_extract_v3.utils import python_callable_method
from dxctechnology.australia_payroll_extract_v3.mapper.time_off_balance_mapper import Time_off_mappper, Active_user_lsl_Timeoffs
from dxctechnology.australia_payroll_extract_v3.mapper.sell_back_mapper import SELL_BACK_MAPPER


def companycode_from_gsap_mapper(export, mapper):

    list_of_codes = list(filter(lambda item: item['Export'] == export, mapper))

    return list_of_codes

def companycode_from_es_mapper(export, mapper):
    date_filter = python_callable_method.check_0015_infotype_for_paygroup()['date']

    list_of_codes = list(filter(lambda item: item['Export'] == export and
                        item['Date'] == date_filter, mapper))

    return list_of_codes

def companycode_from_es_weekly_mapper(export, mapper):
    list_of_codes = list(filter(lambda item: item['Export'] == export, mapper))
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

def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

def process_absence_taken_es_user_conf():
    dag_run_conf = get_dag_run_conf()
    return {
        'location_name': dag_run_conf['location_name'],
        'division_name': dag_run_conf['division_name'],
        'sequence_no_for_2001': dag_run_conf['sequence_no_for_2001'],
        'file_diff': 'CP'
    }

def process_absence_taken_gsap_user_conf():
    dag_run_conf = get_dag_run_conf()
    return {
        'location_name': dag_run_conf['location_name'],
        'division_name': dag_run_conf['division_name'],
        'sequence_no_for_2001': dag_run_conf['sequence_no_for_2001'],
        'file_diff': 'GS'
    }

def process_active_es_user_conf(config):
    return {
        'division_name': config.division_name_es,
        'file_name': config.file_name_prefix + "_" + str(dt.utcnow().strftime("%Y%m%d%H%M%S")) + "_AUREPL_RECP05_DUT8G2I",
        'location_name': 'Australia',
        'sequence_no': '05',
        'region': config.es_region
    }

def process_active_gsap_user_conf(config):
    return {
        'division_name': config.division_name_gsap,
        'file_name': config.file_name_prefix + "_" + str(dt.utcnow().strftime("%Y%m%d%H%M%S")) + "_AUREPL_REGS06_DUT8G2I",
        'location_name': 'Australia',
        'sequence_no': '06',
        'region': config.gsap_region
    }

def process_cashout_es_user_conf(item):
    return{
        'location_name': item['Location'],
        'pay_group_name': item['Paygroup'] if item['Paygroup'] else None,
        'division_name': ['AUES'],
        "date": item['Date'],
        'region': 'ES',
        'sequence_no_for_0416': item['sequence_no_for_0416'],
        'file_diff': 'CP'
    }

def process_cashout_gsap_user_conf(item):
    return{
        'file_format_name': item['File format name'],
        'location_name': item['Location'],
        'region': 'GSAP',
        'division_name': [item['Company code']],
        'sequence_no_for_0416': item['sequence_no_for_0416'],
        'file_diff': 'GS'
    }

def process_payrolldata_export_es_conf(item):
    return {
        'file_format_name': item['File format name'],
        'file_format_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_scripts"
                            ), "displayText", item['File format name'], "uri") if rail.result("get_all_scripts") else None,
        'location_name': item['Location'],
        'pay_group_name': item['Paygroup'] if item['Paygroup'] else None,
        'location_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_locations"
                            ), "displayText", item['Location'], "uri")) if rail.result("get_all_enabled_locations") else None,
        'division_name': item['Company code'],
        'division_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_divisions"
                            ), "displayText", item['Company code'], "uri")) if rail.result("get_all_enabled_divisions") else None,
        'contractor_employee_type_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_employee_type_groups"
                            ), "displayText", 'Contractor', "uri")) if rail.result("get_all_enabled_employee_type_groups") else None,
        'sequence_no_for_2010': item['sequence_no_for_2010'],
        'sequence_no_for_2001': item['sequence_no_for_2001'],
        'date': item['Date']
    }

def process_payrolldata_export_es_weekly_conf(item):
    return {
        'file_format_name': item['File format name'],
        'file_format_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_scripts"
                            ), "displayText", item['File format name'], "uri") if rail.result("get_all_scripts") else None,
        'location_name': item['Location'],
        'pay_group_name': None,
        'location_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_locations"
                            ), "displayText", item['Location'], "uri")) if rail.result("get_all_enabled_locations") else None,
        'division_name': item['Company code'],
        'division_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_divisions"
                            ), "displayText", item['Company code'], "uri")) if rail.result("get_all_enabled_divisions") else None,
        'contractor_employee_type_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_employee_type_groups"
                            ), "displayText", 'Contractor', "uri")) if rail.result("get_all_enabled_employee_type_groups") else None,
        'sequence_no_for_2010': item['sequence_no_for_2010'],
        'sequence_no_for_2001': item['sequence_no_for_2001'],
        'date': None,
        'weekly': True
    }

def process_payrolldata_export_gsap_conf(item):
    return {
        'file_format_name': item['File format name'],
        'file_format_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_scripts"
                            ), "displayText", item['File format name'], "uri")) if rail.result("get_all_scripts") else None,
        'location_code': item['Code'],
        'location_name': item['Location'],
        'location_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_locations"
                            ), "displayText", item['Location'], "uri")) if rail.result("get_all_enabled_locations") else None,
        'division_name': item['Company code'],
        'division_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_divisions"
                            ), "displayText", item['Company code'], "uri")) if rail.result("get_all_enabled_divisions") else None,
        'contractor_employee_type_uri': (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_employee_type_groups"
                            ), "displayText", 'Contractor', "uri")) if rail.result("get_all_enabled_employee_type_groups") else None,
        'sequence_no_for_2010': item['sequence_no_for_2010'],
        'sequence_no_for_2001': item['sequence_no_for_2001']
    }

def process_es_user_schedule_conf(config):
    return {
        'division_name': config.division_name_es,
        'location_name': 'Australia',
        'file_name': config.file_name_prefix + "_" + str(dt.utcnow().strftime("%Y%m%d%H%M%S")) + "_AUREPL_RECP09_DUT8G2I",
        'sequence_no': '09'
    }

def process_gsap_user_schedule_conf(config):
    return {
        'division_name': config.division_name_gsap,
        'location_name': 'Australia',
        'file_name': config.file_name_prefix + "_" + str(dt.utcnow().strftime("%Y%m%d%H%M%S")) + "_AUREPL_REGS12_DUT8G2I",
        'sequence_no': '12'
    }

def get_todays_date():
    date_now = dt.utcnow()
    return {
        "year": date_now.year,
        "month": date_now.month,
        "day": date_now.day
    }

def get_holiday_calander_data(region):
    return {
            "holidayCalendarUri": rail.result("get_all_holiday_calanders")[region],
            "dateRange": {
                "startDate": get_todays_date(),
                "endDate": get_todays_date(),
            }
        }

# pylint: disable=redefined-outer-name
def calculate_dates_for_Report(region, date=None, paygroup=None):
    if region == 'ES':
        if date == 3 and paygroup == 1:
            current_time= dt.now()
            start_date= current_time.replace(day=1) - timedelta(days=1)
            start_date = start_date.replace(day=1).strftime("%m/%d/%Y")

            today= dt.today()
            first_day_of_the_month= today.replace(day=1)
            end_date= (first_day_of_the_month -timedelta(days=1) ).strftime("%m/%d/%Y")

            return {
                'start_date': start_date,
                'end_date': end_date
            }
        if date == 3 and paygroup == 2:
            today= dt.today()
            first_day_of_the_month= today.replace(day=1)
            last_day_of_the_month= first_day_of_the_month -timedelta(days=1)

            start_date= last_day_of_the_month.replace(day=16).strftime("%m/%d/%Y")
            end_date= (first_day_of_the_month -timedelta(days=1) ).strftime("%m/%d/%Y")

            return {
                'start_date': start_date,
                'end_date': end_date
            }
        if date == 23 and paygroup == 2:
            start_date = dt.now().replace(day=1).strftime("%m/%d/%Y")
            end_date = dt.now().replace(day=15).strftime("%m/%d/%Y")

            return {
                'start_date': start_date,
                'end_date': end_date
            }
    if region == 'GSAP':
        weekday= dt.now().weekday()
        start_date = (dt.now() - timedelta(days= weekday +2)).strftime("%m/%d/%Y")
        end_date = (dt.now() + timedelta(days= 4-weekday)).strftime("%m/%d/%Y")

        return {
            'start_date': start_date,
            'end_date': end_date
       }

    return {
            'start_date': None,
            'end_date': None
       }

def get_run_sell_back_balance_report_payload(dag_run):
    if dag_run.conf['region'] == 'ES':
        if dag_run.conf['date']=='03':
            if dag_run.conf['pay_group_name'] == 'AUS-Standard' or dag_run.conf['pay_group_name'] == 'AUS-Standard Mid':
                start_date= calculate_dates_for_Report('ES',3,1)['start_date']
                end_date= calculate_dates_for_Report('ES',3,1)['end_date']

            if dag_run.conf['pay_group_name'] == 'AUS-Semi-Monthly':
                start_date= calculate_dates_for_Report('ES',3,2)['start_date']
                end_date= calculate_dates_for_Report('ES',3,2)['end_date']

        if dag_run.conf['date']=='23' and dag_run.conf['pay_group_name'] == 'AUS-Semi-Monthly':
            start_date= calculate_dates_for_Report('ES',23,2)['start_date']
            end_date= calculate_dates_for_Report('ES',23,2)['end_date']

    if dag_run.conf['region'] == 'GSAP':
        start_date= calculate_dates_for_Report('GSAP')['start_date']
        end_date= calculate_dates_for_Report('GSAP')['end_date']

    get_specific_report_details = rail.result(
        'get_sell_back_balance_report_details')
    filter_values = []

    timeoff_type_uris = rail.result("get_all_timeOffTypes")

    for code in dag_run.conf['division_name']:
        filter_values.append({
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_sell_back_balance_report_details'
                            )['filterConfiguration']['enabledFilters'], 'displayText', 'CurrentDivisionFilter', 'uri'),
            "value": (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_divisions"
                                ), "displayText", code, "uri")).split(':')[-1] if rail.result("get_all_enabled_divisions") else None,
        })

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_sell_back_balance_report_details'
                )['filterConfiguration']['enabledFilters'], 'displayText', 'DateRangeFilter', 'uri'),
        "value": None
    })

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_sell_back_balance_report_details'
                )['filterConfiguration']['enabledFilters'], 'displayText', 'DateRangeFilter', 'uri'),
        "value": start_date
    })

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_sell_back_balance_report_details'
                )['filterConfiguration']['enabledFilters'], 'displayText', 'DateRangeFilter', 'uri'),
        "value": end_date
    })

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_sell_back_balance_report_details'
                )['filterConfiguration']['enabledFilters'], 'displayText', 'CurrentServiceCenterFilter', 'uri'),
        "value": (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_paygroups"
                                ), "displayText", dag_run.conf['pay_group_name'], "uri")).split(':')[-1]
    }) if dag_run.conf['region'] == 'ES' else None

    for timeofftype in timeoff_type_uris:
        if timeofftype:
            filter_values.append({
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_sell_back_balance_report_details'
                                )['filterConfiguration']['enabledFilters'], 'displayText', 'TimeOffTypeFilter', 'uri'),
                "value": timeofftype.split(':')[-1]
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

def get_dates():
    today= dt.today()
    start_date= today.replace(day=1)

    end_date = start_date.replace(day= calendar.monthrange(start_date.year, start_date.month)[1])

    return {
        'start_date': start_date.strftime("%m/%d/%Y"),
        'end_date': end_date.strftime("%m/%d/%Y")
    }

def get_run_user_schedule_balance_report_payload(dag_run):
    get_specific_report_details = rail.result(
        'get_user_schedule_balance_report_details')
    filter_values = []

    for code in dag_run.conf['division_name']:
        filter_values.append({
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_user_schedule_balance_report_details'
                                )['filterConfiguration']['enabledFilters'], 'displayText', 'CurrentDivisionFilter', 'uri'),
                "value": (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_divisions"
                                ), "displayText", code, "uri")).split(':')[-1] if rail.result("get_all_enabled_divisions") else None,
            })

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_user_schedule_balance_report_details'
                )['filterConfiguration']['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri'),
        "value": None
    })

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_user_schedule_balance_report_details'
                )['filterConfiguration']['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri'),
        "value": get_dates()['start_date']
    })

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_user_schedule_balance_report_details'
                )['filterConfiguration']['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri'),
        "value": get_dates()['end_date']
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

def get_run_active_user_balance_report_payload(dag_run):
    get_specific_report_details = rail.result(
        'get_active_user_balance_report_details')
    filter_values = []

    timeoff_type_uris = rail.result("get_all_timeOffTypes")

    for code in dag_run.conf['division_name']:
        filter_values.append({
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_active_user_balance_report_details'
                                )['filterConfiguration']['enabledFilters'], 'displayText', 'CurrentDivisionFilter', 'uri'),
                "value": (rail.find_first_by_attr_and_get_attr(rail.result("get_all_enabled_divisions"
                                ), "displayText", code, "uri")).split(':')[-1] if rail.result("get_all_enabled_divisions") else None,
            })

    for timeofftype in timeoff_type_uris:
        if timeofftype:
            filter_values.append({
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_active_user_balance_report_details'
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


def get_sequence_no(item):
    conf = rail.get_current_context()['dag_run'].conf
    return str(conf[item])


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
    users_data = rail.load_all_records(
        rail.result('compose_item_payroll_csv_file'))
    each_item = "Personal NumberFillerSSNFillerDateTime TypeHoursLCD OrgFiller\n"
    for item in users_data:
        each_item += "".join(list(dict(item).values()))
        each_item += "\n"
    return each_item


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

def get_export_start_date(duration_days,date):
    cut_off_date = dt.strptime(date, "%Y-%m-%d")
    start_date = (dt.utcnow() - timedelta(days=dt.utcnow().weekday())) - timedelta(days=duration_days)
    if start_date < cut_off_date and (rail.get_company_key()).lower() == "dxctechnology":
        return cut_off_date
    return start_date

def get_export_end_date(conf):
    if conf.get('weekly'):
        return dt.utcnow() - timedelta(days=1)

    if conf['pay_group_name'] == 'AUS-Semi-Monthly' and conf['date'] == '23':
        today= dt.today()
        end_date= today.replace(day=15)
        return end_date

    today = date.today().replace(day=1)
    end_date = today.replace(day=1) - timedelta(days=1)
    return end_date

def get_create_payroll_download_batch_payload(duration_days,cut_off_date):
    conf = rail.get_current_context()['dag_run'].conf
    null = None
    employee_type_uris = get_child_hierarchy_data(rail.result('get_contractor_employee_type_child_hierarchy_data'
                                                              ), conf['contractor_employee_type_uri'])

    # Build division + employee-type filter (always present)
    division_and_emptype_filter = {
        "leftExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [
                        conf['division_uri']
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
    }

    # For weekly export (no paygroup), skip service-center filter
    if conf.get('pay_group_name') is None:
        right_filter = division_and_emptype_filter
    else:
        pay_group_names = rail.result("get_pay_groups_data")
        right_filter = {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:service-center"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uris": [f'{pay_group_names}']
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
                            "uris": [
                                conf['division_uri']
                            ]
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
                                    "startDate": {"year": get_export_start_date(duration_days,cut_off_date).strftime("%Y"),
                                                  "month": get_export_start_date(duration_days,cut_off_date).strftime("%m"),
                                                  "day": get_export_start_date(duration_days,cut_off_date).strftime("%d")
                                                  },
                                    "endDate": {"year": get_export_end_date(conf).strftime("%Y"),
                                                "month": get_export_end_date(conf).strftime("%m"),
                                                "day": get_export_end_date(conf).strftime("%d")
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
            "rightExpression": right_filter
        },
        "fileFormatScriptUri": conf['file_format_uri']
    }

def get_create_payroll_download_batch_gsap_payload(duration_days,cut_off_date):
    conf = rail.get_current_context()['dag_run'].conf
    null = None
    child_location_uris = get_child_hierarchy_data(rail.result(
        'get_location_child_hierarchy_data'), conf['location_uri'])
    employee_type_uris = get_child_hierarchy_data(rail.result('get_contractor_employee_type_child_hierarchy_data'
                                                              ), conf['contractor_employee_type_uri'])
    return{
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
                                    "startDate": {"year": get_export_start_date(duration_days,cut_off_date).strftime("%Y"),
                                                  "month": get_export_start_date(duration_days,cut_off_date).strftime("%m"),
                                                  "day": get_export_start_date(duration_days,cut_off_date).strftime("%d")
                                                  },
                                    "endDate": {"year": dt.utcnow().strftime("%Y"),
                                                "month": dt.utcnow().strftime("%m"),
                                                "day": dt.utcnow().strftime("%d")
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
                                "uris": [
                                    conf['division_uri']
                                ]
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

def get_create_payrun_batch_payload(duration_days,cut_off_date):
    conf = rail.get_current_context()['dag_run'].conf
    null = None
    employee_type_uris = get_child_hierarchy_data(rail.result('get_contractor_employee_type_child_hierarchy_data'
                                                              ), conf['contractor_employee_type_uri'])

    # Build division + employee-type filter (always present)
    division_and_emptype_filter = {
        "leftExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [
                        conf['division_uri']
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
    }

    # For weekly export (no paygroup), skip service-center filter
    if conf.get('pay_group_name') is None:
        right_filter = division_and_emptype_filter
    else:
        pay_group_names = rail.result("get_pay_groups_data")
        right_filter = {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:service-center"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uris": [f'{pay_group_names}']
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
                            "uris": [
                                conf['division_uri']
                            ]
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
                                    "startDate": {"year": get_export_start_date(duration_days,cut_off_date).strftime("%Y"),
                                                  "month": get_export_start_date(duration_days,cut_off_date).strftime("%m"),
                                                  "day": get_export_start_date(duration_days,cut_off_date).strftime("%d")
                                                  },
                                    "endDate": {"year": get_export_end_date(conf).strftime("%Y"),
                                                "month": get_export_end_date(conf).strftime("%m"),
                                                "day": get_export_end_date(conf).strftime("%d")
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
            "rightExpression": right_filter
        }
    }

def get_create_payrun_batch_gsap_payload(duration_days,cut_off_date):
    conf = rail.get_current_context()['dag_run'].conf
    null = None
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
                                    "startDate": {"year": get_export_start_date(duration_days,cut_off_date).strftime("%Y"),
                                                  "month": get_export_start_date(duration_days,cut_off_date).strftime("%m"),
                                                  "day": get_export_start_date(duration_days,cut_off_date).strftime("%d")
                                                  },
                                    "endDate": {"year": dt.utcnow().strftime("%Y"),
                                                "month": dt.utcnow().strftime("%m"),
                                                "day": dt.utcnow().strftime("%d")
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
                                "uris": [
                                    conf['division_uri']
                                ]
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
        }
    }


def get_start_date_begin_of_week():
    begin_of_week = dt.utcnow() - timedelta(days=dt.utcnow().weekday())
    return str((begin_of_week - timedelta(days=84)).strftime("%Y-%m-%d"))


def get_end_date_begin_of_week():
    begin_of_week = dt.utcnow() - timedelta(days=dt.utcnow().weekday())
    return str(begin_of_week.strftime("%Y-%m-%d"))


def get_utc_now_date_string(config):
    return dt.utcnow().strftime(config.date_time_format)


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_run_user_report_payload():
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


def getReportPayload(response):
    data = response.json()['d']
    return data['payload']


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_final_users_data_row(items):
    return [items['username'], items['location'],
            items['useruri'],
            items['useruri'].split(':')[-1]
            ]

def get_compose_item_active_user_payroll_aus_data_row(items):

    personnelnumber = ""
    if items['Actual_Employee_ID']:
        personnelnumber = items['Actual_Employee_ID']
    else:
        personnelnumber = items['CLIID']
    return ["P2001",
            personnelnumber,
            "AU",
            "",
            "INS",
            "2001",
            items['Pay_Code_Code'],
            dt.strptime(items['BEGDA'], "%d %B %Y").strftime("%Y%m%d") if items['BEGDA'] else "",
            dt.strptime(items['ENDDA'], "%d %B %Y").strftime("%Y%m%d") if items['ENDDA'] else "",
            "",
            "",
            "",
            "",
            items['Pay_Code_Code'],
            "",
            "",
            items['Pay_Code_Hours'].replace(",","") if items['Pay_Code_Code'] != '5010' else "",
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

def get_user_schedule_balance_us_data_row(items):
    personnelnumber = items['Employee_Id']
    return ["P0007",
            personnelnumber,
            "AU",
            "",
            "INS",
            "0007",
            "",
            dt.strptime(items['Start_date'], "%Y-%m-%d").strftime("%Y%m%d") if items['Start_date'] else "",
            dt.strptime(items['End_date'], "%Y-%m-%d").strftime("%Y%m%d") if items['End_date'] and items[
                                'End_date'] != '99991231' else items['End_date'] if items['End_date'] else "",
            "",
            "",
            "",
            "",
            items['Description'],
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
def get_active_user_balance_us_data_row(items):
    get_user_data_in_query = list(filter(lambda item: item['emp_id']== items['Employee_ID'], map(lambda item:{
        'emp_id': item['Employee_ID'],
        'timeoff_balance': item['Time_Off_Balance'],
        'timeoff_type': items['Time_Off_Type']
    },rail.load_all_records(rail.result("query_active_user_balance_data")))))

    time_off_balance = list(filter(lambda x:  x['Time off type'] == items['Time_Off_Type'], Time_off_mappper))

    personnelnumber = ""
    if items['Actual_Employee_ID']:
        personnelnumber = items['Actual_Employee_ID']
    else:
        personnelnumber = items['Employee_ID']

    timeoff_balance = items['Time_Off_Balance']
    if get_user_data_in_query and get_user_data_in_query[0]['timeoff_type'] in Active_user_lsl_Timeoffs:
        timeoff_balance = str(round(float(get_user_data_in_query[0]['timeoff_balance'].replace(",","")) + float(items['Time_Off_Balance'].replace(",","")),2))

    dag_run_conf = get_dag_run_conf()
    if dag_run_conf['region'] == 'ES':
        start_date = dt.now().replace(day= calendar.monthrange(dt.now().year,dt.now().month)[1]).strftime("%Y%m%d")
    else:
        start_date = dt.now().strftime("%Y%m%d")

    return ["P2006",
            personnelnumber,
            "AU",
            "",
            "INS",
            "2006",
            time_off_balance[0]['Quote Type'],
            start_date,
            dt.now().replace(day= calendar.monthrange(dt.now().year,dt.now().month)[1]).strftime("%Y%m%d"),
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


def get_formated_user_row(item):
    return {
        'username': item['User Name'],
        'location': item['Location (Current)'],
        'useruri': item['UserUri'],
        # 2022-08-30 - for sql date format
        'userenddate': dt.strptime(item['User End Date'], "%d %B %Y").strftime("%Y-%m-%d") if item['User End Date'] else None,
    }.values()


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


def get_sell_back_balance_us_data_row(items):
    sellback_balance = list(filter(lambda x:  x['time_off'] == items['Time_Off_Type'], SELL_BACK_MAPPER))

    personnelnumber = ""
    if items['Actual_Employee_ID']:
        personnelnumber = items['Actual_Employee_ID']
    else:
        personnelnumber = items['Employee_ID']
    return ["P0015",
            personnelnumber,
            "AU",
            "",
            "INS",
            "0015",
            sellback_balance[0]['wage_code'],
            dt.strptime(items['Date'], "%d %B %Y").strftime("%Y%m%d") if items['Date'] else "",
            dt.strptime(items['Date'], "%d %B %Y").strftime("%Y%m%d") if items['Date'] else "",
            "",
            "",
            "",
            "",
            sellback_balance[0]['wage_code'],
            "",
            "AUD",
            abs(float(items['Amount'])),
            "",
            "",
            "",
            "",
            "",
            "",
            ""
        ]

def get_all_required_pacodes(mapper):
    return "'"+"','".join(mapper)+"'"
