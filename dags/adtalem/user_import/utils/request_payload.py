from datetime import datetime, timedelta
import hashlib
import re
from os.path import getsize
from dateutil.relativedelta import relativedelta
import rail

null = None


def get_report_params():
    enabled_filters = rail.result('get_userimport_report_details')[
        'filterConfiguration']['enabledFilters']
    filter_uris = [x['uri']
                   for x in enabled_filters if x['displayText'] == 'UserFilter']
    return {
        'reportParameters': [
            {
                'reportUri': rail.result('get_userimport_report_details')['uri'],
                'filterValues': [
                    {
                        'reportFilterUri': rail.smartjoin_by_delim(filter_uris),
                        'value': '2'
                    }
                ],
                'outputFormatUri': 'urn:replicon:report-output-format-option:csv'
            }
        ]
    }


def do_has_file_content():
    with rail.existing_artifact(rail.result('download_file')) as artifact:
        return getsize(artifact.local_filename) > 0


def get_md5(item):
    column_vals = [str(v)
                   for k, v in item.items() if k not in ('effectivedate', 'encoded')]
    input_reference = hashlib.md5(','.join(column_vals).encode('utf-8'))
    return input_reference.hexdigest()


def get_row_data(item):
    row_data = []
    for k, v in item.items():
        if k == 'encoded':
            row_data.append(get_md5(item))
        elif k in ('jobcode', 'paygroup', 'division', 'salaryhourly', 'regulartemp',
                   'fullparttime', 'state', 'standardhours'):
            row_data.append(v.strip() if v else 'Null')
        else:
            row_data.append(v.strip() if v else '')
    return row_data


def get_customfield_dropdown_option_uris():
    existing_dropdowns_list = rail.result('get_jobcode_dropdowns')
    final_dropdown_list = list(map(lambda x: {
        'target': {
            'uri': x['uri'],
            'name': x['displayText']
        },
        'name': x['displayText'],
        'isEnabled': x['isEnabled']
    }, existing_dropdowns_list)) if existing_dropdowns_list else []

    new_values_to_set = rail.load_all_records(
        rail.result('new_jobcode_values'))

    final_dropdown_list.extend(map(lambda x: {
        'name': x['jobcode'],
        'isEnabled': True
    }, new_values_to_set))

    return final_dropdown_list


def get_employeeid_from_emp_number(employee_number):
    return re.sub('^0+', "", employee_number)


def get_search_user_param():
    return {
        'page': '1',
        'pagesize': '100',
        'columnUris': [
            'urn:replicon:user-list-column:user',
            'urn:replicon:user-list-column:employee-id'
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {
                    'text': rail.result('get_employeeid')
                }
            }
        }
    }


def get_adduser_updateuser_conf(item):
    dnumber = item['dnumber']
    fields_to_consider = ('lastname', 'firstname', 'jobcode', 'jobtitle', 'managerindicator',
                          'paygroup', 'division', 'salaryhourly', 'regulartemp', 'fullparttime',
                          'flsastatus', 'filenumber', 'departmentnumber', 'supervisorpermissionuri',
                          'enduserpermissionuri', 'supervisor_log')

    extra_config = {
        'log': rail.result('create_caribbean_userlog'),
        'slug': rail.get_tenant_slug()
    } if rail.result('create_caribbean_userlog') else {
        'ususer': rail.result('us_user_or_not'),
        'paygrade': item['salarycode'] if item['salarycode'] else null,
        'log': rail.result('create_userlog')
    }

    return {
        **{
            k: v.strip() if v else null for k, v in item.items() if k in fields_to_consider
        },
        **{
            'employeeid': item['employeenumber'],
            'loginname': dnumber,
            'activeleavestatus': item['employeestatus'],
            'supervisor': item['managerdnumber'],
            'emailaddress': item['businessemailaddress'],
            'homestate': item['state'] if item['state'] else 'Null',
            'standardhours': item['standardhours'] if item['standardhours'] else 'Null',
            'startdate': item['hiredate'].replace('-', '/'),
            'rehiredate': item['rehiredate'].replace('-', '/'),
            'servicedate': item['servicedate'].replace('-', '/'),
            'effectivedate': item['effectivedate'].replace('-', '/'),
            'terminationdate': item['terminationdate'].replace('-', '/'),
            'colleaguednumber': dnumber,
            'worklocation': item['worklocationname'],
            'jobfunction': item['jobfunctionname'],
            'useruri': rail.result('get_required_useruri')
        },
        **extra_config
    }


def get_datetime_obj(date_str, fmt='%m/%d/%Y'):
    datetime_obj = datetime.strptime(date_str, fmt)
    return {
        'year': datetime_obj.year,
        'month': datetime_obj.month,
        'day': datetime_obj.day
    }


def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_createuser_payload(dag_run):
    start_date = get_datetime_obj(dag_run.conf['startdate'])
    return {
        'user': {
            'target': {
                'loginName': dag_run.conf['loginname']
            },
            'firstname': dag_run.conf['firstname'],
            'lastname': dag_run.conf['lastname'],
            'emailAddress': dag_run.conf['emailaddress'],
            'employeeId': dag_run.conf['employeeid'],
            'department': {
                'name': 'Adtalem'
            },
            'employmentDateRange': {
                'startDate': start_date,
            },
            'securityConfiguration': {
                'enabledAuthenticationTypeUris': [
                    'urn:replicon:user-authentication-type:sso'
                ],
                'isLoginEnabled': 'true',
                'loginName': dag_run.conf['loginname'],
                'SSOName': dag_run.conf['loginname']
            },
            'permissionSets': [{
                'uri': dag_run.conf['enduserpermissionuri']
            }],
            'employeeType': {
                'uri': rail.result('get_required_employeetypeuri')
            }
        }
    }


def get_putdropdownoption_jobcode(dag_run):

    jobcode_dropdownoptions = []

    existing_jobcode_dropdowns = rail.result(
        'get_jobcode_dropdown', 'dropdowns')

    jobcode_dropdownoptions = list(map(lambda x: {
        'target': {
            'uri': x['uri'],
            'name': x['displayText']
        },
        'name': x['displayText'],
        'isEnabled': x['isEnabled']
    }, existing_jobcode_dropdowns)) if existing_jobcode_dropdowns else []

    jobcode_dropdownoptions.append({
        'target': {
            'name': dag_run.conf['jobcode']
        },
        'name': dag_run.conf['jobcode'],
        'isEnabled': True
    })

    return {
        "customFieldUri": rail.result('get_required_user_customfields')['job_code'],
        "customFieldDropDownOptionUris": jobcode_dropdownoptions
    }


def get_batchid(salaryhourly, paygroup):
    batchid = ''
    if paygroup:
        if salaryhourly == 'S':
            batchid = f"{paygroup}SAL"
        elif salaryhourly == 'H':
            batchid = f"{paygroup}NON"
    return batchid


def get_assign_sicktimeoff_policy(dag_run):
    rehire_date_plus_90_days = datetime.strptime(
        dag_run.conf['rehiredate'], '%m/%d/%Y') + timedelta(days=90)

    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('get_timeofftype_uris_to_assign', 'sick_timeoffname_uri')
        },
        "policySetScheduleEntries": [
            {
                "effectiveDate": {
                    "year": rehire_date_plus_90_days.year,
                    "month": rehire_date_plus_90_days.month,
                    "day": rehire_date_plus_90_days.day
                },
                "description": f"Effective on {rehire_date_plus_90_days.month}-{rehire_date_plus_90_days.day}-{rehire_date_plus_90_days.year}",
                **rail.result('get_policy_schedule')['poliset']
            }
        ]
    }


def get_assign_sicktimeoff_policy2(dag_run):
    rehire_date = datetime.strptime(
        dag_run.conf['rehiredate'], '%m/%d/%Y')

    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('get_timeofftype_uris_to_assign', 'sick_timeoffname_uri')
        },
        "policySetScheduleEntries": [
            {
                "effectiveDate": {
                    "year": rehire_date.year,
                    "month": rehire_date.month,
                    "day": rehire_date.day
                },
                "description": f"Effective on {rehire_date.month}-{rehire_date.day}-{rehire_date.year}",
                **rail.result('get_policy_schedule')['poliset']
            }
        ]
    }


def get_assign_pto_policy(dag_run):
    rehire_date = datetime.strptime(
        dag_run.conf['rehiredate'], '%m/%d/%Y')
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('get_pto_timeofftype_uri')
        },
        "policySetScheduleEntries": [
            {
                "effectiveDate": {
                    "year": rehire_date.year,
                    "month": rehire_date.month,
                    "day": rehire_date.day
                },
                "description": f"Effective on {rehire_date.month}-{rehire_date.day}-{rehire_date.year}",
                "policySet": rail.result('get_pto_policyset')
            }
        ]
    }


# pylint: disable=too-many-arguments
def get_put_vacation_policy(useruri, first_totype_uri, datestr, policyname,
                            previouspolicy=None,
                            policymapper_task_name='get_required_vacation_timeoff_policyschedule'):

    date_obj = get_datetime_obj(datestr)

    if policyname in ('RFT-CH', 'RFT-CH-CA', 'RFT-DY', 'RFT-DY-CA'):

        return {
            "timeOffAccount": {
                "userUri": useruri,
                "timeOffTypeUri": first_totype_uri
            },
            "policySetScheduleEntries":
                [previouspolicy] +
                [{
                    "effectiveDate": date_obj,
                    "description": f"Effective on {date_obj['month']}-{date_obj['day']}-{date_obj['year']}",
                    **rail.result(policymapper_task_name)['0 - 5']
                }] if previouspolicy else [
                {
                    "effectiveDate": date_obj,
                    "description": f"Effective on {date_obj['month']}-{date_obj['day']}-{date_obj['year']}",
                    **rail.result(policymapper_task_name)['0 - 5']
                }
            ]
        }

    date_plus_5years = datetime.strptime(
        datestr, '%m/%d/%Y') + relativedelta(months=+60)
    date_plus_10years = datetime.strptime(
        datestr, '%m/%d/%Y') + relativedelta(months=+120)

    return {
        "timeOffAccount": {
            "userUri": useruri,
            "timeOffTypeUri": first_totype_uri
        },
        "policySetScheduleEntries":
            [previouspolicy] +
            [{
                "effectiveDate": date_obj,
                "description": f"Effective on {date_obj['month']}-{date_obj['day']}-{date_obj['year']}",
                **rail.result(policymapper_task_name)['0 - 5']
            },
            {
                "effectiveDate": {
                    "year": date_plus_5years.year,
                    "month": date_plus_5years.month,
                    "day": date_plus_5years.day
                },
                "description": f"Effective on {date_plus_5years.month}-{date_plus_5years.day}-{date_plus_5years.year}",
                **rail.result(policymapper_task_name)['5 - 9']
            },
            {
                "effectiveDate": {
                    "year": date_plus_10years.year,
                    "month": date_plus_10years.month,
                    "day": date_plus_10years.day
                },
                "description": f"Effective on {date_plus_10years.month}-{date_plus_10years.day}-{date_plus_10years.year}",
                **rail.result(policymapper_task_name)['10 +']
            }] if previouspolicy else [
            {
                "effectiveDate": date_obj,
                "description": f"Effective on {date_obj['month']}-{date_obj['day']}-{date_obj['year']}",
                **rail.result(policymapper_task_name)['0 - 5']
            },
            {
                "effectiveDate": {
                    "year": date_plus_5years.year,
                    "month": date_plus_5years.month,
                    "day": date_plus_5years.day
                },
                "description": f"Effective on {date_plus_5years.month}-{date_plus_5years.day}-{date_plus_5years.year}",
                **rail.result(policymapper_task_name)['5 - 9']
            },
            {
                "effectiveDate": {
                    "year": date_plus_10years.year,
                    "month": date_plus_10years.month,
                    "day": date_plus_10years.day
                },
                "description": f"Effective on {date_plus_10years.month}-{date_plus_10years.day}-{date_plus_10years.year}",
                **rail.result(policymapper_task_name)['10 +']
            }
        ]
    }


def get_user_tenure(servicedate, rehire_date=null):

    servicedatetime = datetime.strptime(servicedate, '%m/%d/%Y')
    difference_datetime = (datetime.strptime(
        rehire_date, '%m/%d/%Y') - servicedatetime) if rehire_date else (datetime.now() - servicedatetime)
    return int((difference_datetime.total_seconds() / 86400) / 365)


# pylint: disable=too-many-arguments
def get_put_vacation_policy_enabled(useruri, first_totype_uri, servicedate, policyname,
                                    previouspolicy, policymapper_task_name):
    date_obj = get_today_date()

    if policyname in ('RFT-CH', 'RFT-CH-CA', 'RFT-DY', 'RFT-DY-CA'):

        return {
            "timeOffAccount": {
                "userUri": useruri,
                "timeOffTypeUri": first_totype_uri
            },
            "policySetScheduleEntries":
                [previouspolicy] +
                [{
                    "effectiveDate": date_obj,
                    "description": f"Effective on {date_obj['month']}-{date_obj['day']}-{date_obj['year']}",
                    **rail.result(policymapper_task_name)['0 - 5']
                }] if previouspolicy else [
                {
                    "effectiveDate": date_obj,
                    "description": f"Effective on {date_obj['month']}-{date_obj['day']}-{date_obj['year']}",
                    **rail.result(policymapper_task_name)['0 - 5']
                }
            ]
        }

    date_plus_5years = datetime.now() + relativedelta(months=+60)
    date_plus_10years = datetime.now() + relativedelta(months=+120)
    user_tenure = get_user_tenure(servicedate)

    if user_tenure <= 5:
        return {
            "timeOffAccount": {
                "userUri": useruri,
                "timeOffTypeUri": first_totype_uri
            },
            "policySetScheduleEntries":
                [previouspolicy] +
                [{
                    "effectiveDate": date_obj,
                    "description": f"Effective on {date_obj['month']}-{date_obj['day']}-{date_obj['year']}",
                    **rail.result(policymapper_task_name)['0 - 5']
                },
                {
                    "effectiveDate": {
                        "year": date_plus_5years.year,
                        "month": date_plus_5years.month,
                        "day": date_plus_5years.day
                    },
                    "description": f"Effective on {date_plus_5years.month}-{date_plus_5years.day}-{date_plus_5years.year}",
                    **rail.result(policymapper_task_name)['5 - sep']
                },
                {
                    "effectiveDate": {
                        "year": date_plus_10years.year,
                        "month": date_plus_10years.month,
                        "day": date_plus_10years.day
                    },
                    "description": f"Effective on {date_plus_10years.month}-{date_plus_10years.day}-{date_plus_10years.year}",
                    **rail.result(policymapper_task_name)['10 +']
                }] if previouspolicy else [
                {
                    "effectiveDate": date_obj,
                    "description": f"Effective on {date_obj['month']}-{date_obj['day']}-{date_obj['year']}",
                    **rail.result(policymapper_task_name)['0 - 5']
                },
                {
                    "effectiveDate": {
                        "year": date_plus_5years.year,
                        "month": date_plus_5years.month,
                        "day": date_plus_5years.day
                    },
                    "description": f"Effective on {date_plus_5years.month}-{date_plus_5years.day}-{date_plus_5years.year}",
                    **rail.result(policymapper_task_name)['5 - sep']
                },
                {
                    "effectiveDate": {
                        "year": date_plus_10years.year,
                        "month": date_plus_10years.month,
                        "day": date_plus_10years.day
                    },
                    "description": f"Effective on {date_plus_10years.month}-{date_plus_10years.day}-{date_plus_10years.year}",
                    **rail.result(policymapper_task_name)['10 +']
                }
            ]
        }
    if 5 < user_tenure < 10:
        return {
            "timeOffAccount": {
                "userUri": useruri,
                "timeOffTypeUri": first_totype_uri
            },
            "policySetScheduleEntries":
                [previouspolicy] +
                [{
                    "effectiveDate": {
                        "year": date_plus_5years.year,
                        "month": date_plus_5years.month,
                        "day": date_plus_5years.day
                    },
                    "description": f"Effective on {date_plus_5years.month}-{date_plus_5years.day}-{date_plus_5years.year}",
                    **rail.result(policymapper_task_name)['5 - sep']
                },
                {
                    "effectiveDate": {
                        "year": date_plus_10years.year,
                        "month": date_plus_10years.month,
                        "day": date_plus_10years.day
                    },
                    "description": f"Effective on {date_plus_10years.month}-{date_plus_10years.day}-{date_plus_10years.year}",
                    **rail.result(policymapper_task_name)['10 +']
                }] if previouspolicy else [
                {
                    "effectiveDate": {
                        "year": date_plus_5years.year,
                        "month": date_plus_5years.month,
                        "day": date_plus_5years.day
                    },
                    "description": f"Effective on {date_plus_5years.month}-{date_plus_5years.day}-{date_plus_5years.year}",
                    **rail.result(policymapper_task_name)['5 - sep']
                },
                {
                    "effectiveDate": {
                        "year": date_plus_10years.year,
                        "month": date_plus_10years.month,
                        "day": date_plus_10years.day
                    },
                    "description": f"Effective on {date_plus_10years.month}-{date_plus_10years.day}-{date_plus_10years.year}",
                    **rail.result(policymapper_task_name)['10 +']
                }
            ]
        }
    return {
        "timeOffAccount": {
            "userUri": useruri,
            "timeOffTypeUri": first_totype_uri
        },
        "policySetScheduleEntries":
            [previouspolicy] +
            [{
                "effectiveDate": {
                    "year": date_plus_10years.year,
                    "month": date_plus_10years.month,
                    "day": date_plus_10years.day
                },
                "description": f"Effective on {date_plus_10years.month}-{date_plus_10years.day}-{date_plus_10years.year}",
                **rail.result(policymapper_task_name)['10 +']
            }] if previouspolicy else [
            {
                "effectiveDate": {
                    "year": date_plus_10years.year,
                    "month": date_plus_10years.month,
                    "day": date_plus_10years.day
                },
                "description": f"Effective on {date_plus_10years.month}-{date_plus_10years.day}-{date_plus_10years.year}",
                **rail.result(policymapper_task_name)['10 +']
            }
        ]
    }


def get_put_timeoffpolicywithinitialbalance_plus_16days(dag_run):

    effective_date = datetime.strptime(
        dag_run.conf['terminationdate'], '%m/%d/%Y') if dag_run.conf['terminationdate'] else datetime.now()

    effective_date_plus_16_days = effective_date + timedelta(days=16)

    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri']
        },
        "policySetScheduleEntries": rail.result('past_policyset_schedule') +
        [{
            "effectiveDate": {
                "year": effective_date_plus_16_days.year,
                "month": effective_date_plus_16_days.month,
                "day": effective_date_plus_16_days.day
            },
            "description": f"Effective on {effective_date_plus_16_days.month}/{effective_date_plus_16_days.day}/{effective_date_plus_16_days.year}",
            "policySet": {
                "timeOffBalanceEventScripts": [
                    {
                        "scriptTarget": {
                            "uri": rail.result('get_startingbalance_script_uri')
                        },
                        "additionalParameters": [
                            {
                                "keyUri": "urn:replicon:script-key:parameter:amount",
                                "value": {
                                    "number": 0
                                }
                            },
                            {
                                "keyUri": "urn:replicon:script-key:parameter:precedence",
                                "value": {
                                    "number": 20
                                }
                            }
                        ]
                    }
                ],
                "timeOffValidationScripts": []
            }
        }]
    }


def get_put_timeoffpolicywithinitialbalance(dag_run):

    effective_date = datetime.now()

    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri']
        },
        "policySetScheduleEntries": rail.result('past_policyset_schedule') +
        [{
            "effectiveDate": {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            },
            "description": f"Effective on {effective_date.month}/{effective_date.day}/{effective_date.year}",
            "policySet": {
                "timeOffBalanceEventScripts": [
                    {
                        "scriptTarget": {
                            "uri": rail.result('get_startingbalance_script_uri')
                        },
                        "additionalParameters": [
                            {
                                "keyUri": "urn:replicon:script-key:parameter:amount",
                                "value": {
                                    "number": 0
                                }
                            },
                            {
                                "keyUri": "urn:replicon:script-key:parameter:precedence",
                                "value": {
                                    "number": 20
                                }
                            }
                        ]
                    }
                ],
                "timeOffValidationScripts": []
            }
        }]
    }


def get_createuser_payload_caribbean_user(dag_run):
    start_date = get_datetime_obj(dag_run.conf['startdate'])
    return {
        'user': {
            'target': {
                'loginName': dag_run.conf['loginname']
            },
            'firstname': dag_run.conf['firstname'],
            'lastname': dag_run.conf['lastname'],
            'emailAddress': dag_run.conf['emailaddress'] if '@' in dag_run.conf['emailaddress'] else null,
            'employeeId': dag_run.conf['employeeid'],
            'department': {
                'name': 'Adtalem'
            },
            'employmentDateRange': {
                'startDate': start_date,
            },
            'securityConfiguration': {
                'enabledAuthenticationTypeUris': [
                    'urn:replicon:user-authentication-type:sso'
                ],
                'isLoginEnabled': 'true',
                'loginName': dag_run.conf['loginname']
            },
            'permissionSets': [{
                'uri': dag_run.conf['enduserpermissionuri']
            }],
            'employeeType': {
                'uri': rail.result('get_required_employeetypeuri')
            }
        }
    }


def get_put_time_offpolicy_with_initial_balance_blank(dag_run):

    effective_date = datetime.strptime(
        dag_run.conf['terminationdate'], '%m/%d/%Y')
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri']
        },
        "policySetScheduleEntries": [
            rail.result('past_policyset_schedule') +
            [{
                "effectiveDate": {
                    "year": effective_date.year,
                    "month": effective_date.month,
                    "day": effective_date.day
                },
                "description": f"Effective on {effective_date.month}/{effective_date.day}/{effective_date.year}",
                "policySet":  {
                    "timeOffBalanceEventScripts": [],
                    "timeOffValidationScripts": [
                        {
                            "scriptTarget": {
                                "uri": rail.result('get_preventbalanceoverdraw_script_uri')
                            },
                            "additionalParameters": [
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:amount",
                                    "value": {
                                        "number": "0"
                                    }
                                },
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:precedence",
                                    "value": {
                                        "number": "20"
                                    }
                                }
                            ]
                        }
                    ]
                }
            }]
        ]
    }


def get_put_timeoffpolicywithinitialbalance_terminationdate(dag_run):

    effective_date = datetime.strptime(
        dag_run.conf['terminationdate'], '%m/%d/%Y')
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri']
        },
        "policySetScheduleEntries": rail.result('past_policyset_schedule') +
        [{
            "effectiveDate": {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            },
            "description": f"Effective on {effective_date.month}/{effective_date.day}/{effective_date.year}",
            "policySet": {
                "timeOffBalanceEventScripts": [
                    {
                        "scriptTarget": {
                            "uri": rail.result('get_startingbalance_script_uri')
                        },
                        "additionalParameters": [
                            {
                                "keyUri": "urn:replicon:script-key:parameter:amount",
                                "value": {
                                    "number": 0
                                }
                            },
                            {
                                "keyUri": "urn:replicon:script-key:parameter:precedence",
                                "value": {
                                    "number": 20
                                }
                            }
                        ]
                    }
                ],
                "timeOffValidationScripts": []
            }
        }]
    }
