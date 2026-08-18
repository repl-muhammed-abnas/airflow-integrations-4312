from datetime import datetime, timedelta
import hashlib
import rail


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_md5(item):
    column_vals = [str(x) for x in item.values()]
    input_reference = hashlib.md5(''.join(column_vals).encode('utf-8'))
    return input_reference.hexdigest()


def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_replicon_date(date_str, fmt='%m/%d/%Y'):
    datetime_obj = datetime.strptime(date_str, fmt)
    return {
        'year': datetime_obj.year,
        'month': datetime_obj.month,
        'day': datetime_obj.day
    }


def get_row_data(item):
    row_data = []
    for k, v in item.items():
        if k == 'md5_reference':
            row_data.append(get_md5(item))
        else:
            row_data.append(v.strip() if v else '')
    return row_data


def get_add_update_user_conf(item):
    return {
        k.lower(): v for k, v in item.items() if k not in ('_ancestry', '_ecid', '_replication_position')
    }


def get_jobtitle_dropdown_options_payload():

    replicon_jobtitle_dropdowns = rail.result(
        'get_existing_jobtitle_customfields')

    final_dropdown_list = list(map(lambda x: {
        'target': {
            'uri': x['uri']
        },
        'name': x['displayText'],
        'isEnabled': x['isEnabled']
    }, replicon_jobtitle_dropdowns)) if replicon_jobtitle_dropdowns else []

    new_jobtitle_dropdowns = rail.load_all_records(
        rail.result('query_jobtitle_to_create'))

    final_dropdown_list.extend(map(lambda x: {
        'target': {
            'name': x['Job_Title']
        },
        'name': x['Job_Title'],
        'isEnabled': True
    }, new_jobtitle_dropdowns))

    return final_dropdown_list


def write_unchanged_logs(item):
    employeenumber = item['employeenumber']
    user_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('parse_report_data'), 'Login Name', employeenumber, 'useruri', '')
    return {
        'loginname': employeenumber,
        'uri': user_uri,
        'action': 'Ignored',
        'status': 'Skipped',
        'reason': 'No change found from previous input file'
    }


def get_update_terminate_costcenter(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['uri']
        },
        "modifications": {
            "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementCostCenterSchedule": [],
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {
                                "uri": rail.result('get_terminatecostcenter')
                            },
                            "effectiveDate": get_today_date()
                        }
                    ]
                }
            }
        }
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


def get_alltimeoff_after_enddate_payload(dag_run):

    enddate_plus_1 = datetime.strptime(
        dag_run.conf['enddate'], '%d/%m/%Y') + timedelta(days=1)
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": [
            "urn:replicon:time-off-list-column:time-off",
            "urn:replicon:time-off-list-column:start-date",
            "urn:replicon:time-off-list-column:end-date"
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": {
                                "day": enddate_plus_1.day,
                                "month": enddate_plus_1.month,
                                "year": enddate_plus_1.year
                            }
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-owner"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uri": dag_run.conf['useruri']
                    }
                }
            }
        }
    }


def get_holidaybookings_delete_payload(dag_run):
    return {
        "page": 1,
        "pagesize": 1000,
        "columnUris": [
            "urn:replicon:time-off-list-column:time-off",
            "urn:replicon:time-off-list-column:start-date",
            "urn:replicon:time-off-list-column:end-date"
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": get_today_date()
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-owner"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uri": dag_run.conf['useruri']
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-type"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uri": rail.result('get_holiday_timeoff_uri')
                        }
                    }
                }
            }
        }
    }


def get_put_timeoff_with_initialblank(dag_run):

    effective_date = get_replicon_date(dag_run.conf['effectivedate'])
    if rail.result('past_policyset_schedule')['prevent_overdrawvalue_flag']:
        policy_set_schedule_entries = rail.result(
            'past_policyset_schedule')['policy_schedule_entries'] + [{
                "effectiveDate": effective_date,
                "description": f"Effective on {effective_date['month']}/{effective_date['day']}/{effective_date['year']}",
                "policySet":  {
                    "timeOffBalanceEventScripts": [],
                    "timeOffValidationScripts": [
                        {
                            "scriptTarget": {
                                "uri": rail.result('get_preventbalanceoverdraw_script')
                            },
                            "additionalParameters": [
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                                    "value": {
                                        "number": rail.result('past_policyset_schedule')['prevent_overdrawvalue']
                                    }
                                },
                                {
                                    "keyUri": "urn:replicon:script-key:parameter:precedence",
                                    "value": {
                                        "number": "0"
                                    }
                                }
                            ]
                        }
                    ]
                }
            }]
    else:
        policy_set_schedule_entries = rail.result(
            'past_policyset_schedule')['policy_schedule_entries'] + [{
                "effectiveDate": effective_date,
                "description": f"Effective on {effective_date['month']}/{effective_date['day']}/{effective_date['year']}"
            }]

    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri']
        },
        "policySetScheduleEntries": policy_set_schedule_entries
    }


def get_put_simplepattern_updateuser(dag_run):
    full_time_availability = int(dag_run.conf['full_time_availability'])
    weekly_hours = 40 * (full_time_availability * 0.01)
    daily_hours = str(round(float(weekly_hours * 0.5), 2))
    decimal_val_rounded = int(str(daily_hours).rsplit('.', maxsplit=1)[-1])
    number_val_rounded = int(str(daily_hours).split('.', maxsplit=1)[0])
    final_decimal_val_daily_hrs = 100
    if 0 <= decimal_val_rounded < 15:
        final_decimal_val_daily_hrs = 0
    if 15 < decimal_val_rounded <= 35:
        final_decimal_val_daily_hrs = 25
    if 35 < decimal_val_rounded <= 65:
        final_decimal_val_daily_hrs = 50
    if 65 < decimal_val_rounded <= 85:
        final_decimal_val_daily_hrs = 75
    if decimal_val_rounded > 85:
        final_decimal_val_daily_hrs = 100
    final_dailyval_hours = number_val_rounded + \
        1 if final_decimal_val_daily_hrs == 100 else number_val_rounded
    final_dailyval_mins = 0 if final_decimal_val_daily_hrs == 100 else int(
        (final_decimal_val_daily_hrs * 0.1) * 60)
    return {
        "officeScheduleUri": rail.result('create_officeschedule_draft'),
        "pattern": {
            "startDayOfWeekUri": "urn:replicon:day-of-week:sunday",
            "day2WorkDuration": {
                "hours": final_dailyval_hours,
                "minutes": final_dailyval_mins,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0
            },
            "day3WorkDuration": {
                "hours": final_dailyval_hours,
                "minutes": final_dailyval_mins,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0
            },
            "day4WorkDuration": {
                "hours": final_dailyval_hours,
                "minutes": final_dailyval_mins,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0
            },
            "day5WorkDuration": {
                "hours": final_dailyval_hours,
                "minutes": final_dailyval_mins,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0
            },
            "day6WorkDuration": {
                "hours": final_dailyval_hours,
                "minutes": final_dailyval_mins,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0
            }
        }
    }


def get_timeoff_update_rehire_v2(item, dag_run):

    all_timeoff_type_var = rail.result('get_variables')['all_timeoff_type']
    floating_holiday_var = rail.result('get_variables')['floating_holiday_var']
    effectivedate_toconsider = get_today_date()
    if rail.result('get_costcenterschedule_to_assign'):
        effectivedate_toconsider = get_replicon_date(
            dag_run.conf['assignment_status_effective_date'])
    if rail.result('get_servicecenterschedule_to_assign'):
        effectivedate_toconsider = get_replicon_date(
            dag_run.conf['assignment_category_effective_date'])

    return {
        **{
            k.lower(): v for k, v in dag_run.conf.items() if k not in ('assignment_status_effective_date',
                                                                       'assignment_category_effective_date',
                                                                       'supervisor_log', 'reporturi', 'reportfilteruri',
                                                                       'supervisor_permissionuri',
                                                                       '_ancestry', '_ecid', '_replication_position')
        },
        'timeoffuri': item['uri'],
        'isarehire': 'Yes' if get_task_state('enable_login') == 'success' else 'No',
        'timeofftypename': item['name'],
        'effectivedate_toconsider': effectivedate_toconsider,
        'all_timeoff_type_var': all_timeoff_type_var,
        'floating_holiday_var': floating_holiday_var
    }


def process_timeoff_update_rehire_check(dag_run):
    all_timeoff_type = dag_run.conf['all_timeoff_type_var']
    floating_holiday = dag_run.conf['floating_holiday_var']
    isarehire = dag_run.conf['isarehire']
    name = dag_run.conf['timeofftypename']
    return (all_timeoff_type == 'yes' and name != 'Floating Holiday') or (
        floating_holiday == 'yes' and name == 'Floating Holiday') or (
        floating_holiday == 'no' and name == 'Floating Holiday' and isarehire == 'Yes')


def get_assign_timeoff_policy_request(dag_run):

    useruri = dag_run.conf['useruri']
    timeoff_uri = dag_run.conf['timeoffuri']
    isarehire = dag_run.conf['isarehire']

    rehire_date = get_replicon_date(dag_run.conf['rehiredate']) if isarehire == 'Yes' else \
        dag_run.conf['effectivedate_toconsider']

    policyset_schedule_entries = rail.result('past_policyset_schedule') + [{
        "effectiveDate": rehire_date,
        "description": f"Effective On {rehire_date['month']}/{rehire_date['day']}/{rehire_date['year']}",
        "policySet": rail.result('get_defaultpolicy_from_global_level')['policy_sets'][0]
    }] if rail.result('past_policyset_schedule') else [{
        "effectiveDate": rehire_date,
        "description": f"Effective On {rehire_date['month']}/{rehire_date['day']}/{rehire_date['year']}",
        "policySet": rail.result('get_defaultpolicy_from_global_level')['policy_sets'][0]
    }]

    return {
        "timeOffAccount": {
            "userUri": useruri,
            "timeOffTypeUri": timeoff_uri
        },
        "policySetScheduleEntries": policyset_schedule_entries
    }


def get_assign_timeoff_policy_request2(dag_run):

    useruri = dag_run.conf['useruri']
    timeoff_uri = dag_run.conf['timeoffuri']
    isarehire = dag_run.conf['isarehire']

    rehire_date = get_replicon_date(dag_run.conf['rehiredate']) if isarehire == 'Yes' else \
        dag_run.conf['effectivedate_toconsider']

    policyset_schedule_entries = rail.result('past_policyset_schedule') + [{
        "effectiveDate": rehire_date,
        "description": f"Effective On {rehire_date['month']}/{rehire_date['day']}/{rehire_date['year']}",
        "policySet": rail.result('final_policy_to_assign')
    }] if rail.result('past_policyset_schedule') else [{
        "effectiveDate": rehire_date,
        "description": f"Effective On {rehire_date['month']}/{rehire_date['day']}/{rehire_date['year']}",
        "policySet": rail.result('final_policy_to_assign')
    }]

    return {
        "timeOffAccount": {
            "userUri": useruri,
            "timeOffTypeUri": timeoff_uri
        },
        "policySetScheduleEntries": policyset_schedule_entries
    }


def get_assign_timeoff_policy_request3(dag_run):

    useruri = dag_run.conf['useruri']
    timeoff_uri = dag_run.conf['timeoffuri']

    return {
        "timeOffAccount": {
            "userUri": useruri,
            "timeOffTypeUri": timeoff_uri
        },
        "policySetScheduleEntries": rail.result('past_policyset_schedule') + rail.result(
            'get_final_paidtimeoff_policysets') if rail.result('past_policyset_schedule') else rail.result(
            'get_final_paidtimeoff_policysets')
    }


def get_createuser_payload(dag_run):
    start_date_obj = get_replicon_date(dag_run.conf['startdate'])
    permission_sets_to_assign = []
    if dag_run.conf['project_manager_permissionuri']:
        permission_sets_to_assign.append({
            'uri': dag_run.conf['project_manager_permissionuri']
        })
    if dag_run.conf['project_manager_permissionuri']:
        permission_sets_to_assign.append({
            'uri': dag_run.conf['project_resource_reports_permissionuri']
        })
    policy_sets_to_assign = list(map(lambda x: {
        'uri': x
    }, rail.result('get_required_policysets_to_assign')))
    return {
        "user": {
            "target": {
                "loginName": dag_run.conf['employeenumber']
            },
            "firstname": dag_run.conf['firstname'],
            "lastname": dag_run.conf['lastname'],
            "emailAddress": dag_run.conf['emailaddress'],
            "employeeId": dag_run.conf['employeenumber'],
            "schedulePolicySchedule": [],
            "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
            "employmentDateRange": {
                "startDate": start_date_obj
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['employeenumber']
            },
            "permissionSets": permission_sets_to_assign,
            "policySets": policy_sets_to_assign,
            "timeZone": {
                "uri": rail.result('get_required_timezone_uri')
            },
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "uri": dag_run.conf['departmentgroupuri']
                    }
                }
            ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": rail.result('get_required_employeetype')
                    }
                }
            ],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "name": "Weekly"
                    }
                }
            ],
            "payRuleScriptSchedule": [
                {
                    "payRuleScript": {
                        "uri": rail.result('get_required_payrule_uri')
                    }
                }
            ]
        }
    }


def get_put_simplepattern_adduser(dag_run):
    full_time_availability = int(dag_run.conf['full_time_availability'])
    weekly_hours = 40 * (full_time_availability * 0.01)
    daily_hours = str(round(float(weekly_hours / 5), 3))
    decimal_val_rounded = int(str(daily_hours).rsplit('.', maxsplit=1)[-1])
    decimal_val_rounded = int(str(decimal_val_rounded) + '0') if len(
        str(decimal_val_rounded)) < 2 else int(str(decimal_val_rounded))
    number_val_rounded = int(str(daily_hours).split('.', maxsplit=1)[0])
    final_decimal_val_daily_hrs = 60
    if 0 <= decimal_val_rounded < 7.4:
        final_decimal_val_daily_hrs = 0
    if 7.5 < decimal_val_rounded <= 22.5:
        final_decimal_val_daily_hrs = 15
    if 22.5 < decimal_val_rounded <= 37.5:
        final_decimal_val_daily_hrs = 30
    if 37.5 < decimal_val_rounded <= 52.5:
        final_decimal_val_daily_hrs = 45
    final_dailyval_hours = number_val_rounded + \
        1 if final_decimal_val_daily_hrs == 60 else number_val_rounded
    final_dailyval_mins = 0 if final_decimal_val_daily_hrs == 60 else final_decimal_val_daily_hrs
    return {
        "officeScheduleUri": rail.result('create_officeschedule_draft'),
        "pattern": {
            "startDayOfWeekUri": "urn:replicon:day-of-week:sunday",
            "day2WorkDuration": {
                "hours": final_dailyval_hours,
                "minutes": final_dailyval_mins,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0
            },
            "day3WorkDuration": {
                "hours": final_dailyval_hours,
                "minutes": final_dailyval_mins,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0
            },
            "day4WorkDuration": {
                "hours": final_dailyval_hours,
                "minutes": final_dailyval_mins,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0
            },
            "day5WorkDuration": {
                "hours": final_dailyval_hours,
                "minutes": final_dailyval_mins,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0
            },
            "day6WorkDuration": {
                "hours": final_dailyval_hours,
                "minutes": final_dailyval_mins,
                "seconds": 0,
                "milliseconds": 0,
                "microseconds": 0
            }
        }
    }


def get_timeoff_adduser_conf(item, dag_run):

    return {
        **{
            k.lower(): v for k, v in dag_run.conf.items() if k not in ('assignment_status_effective_date',
                                                                       'assignment_category_effective_date',
                                                                       'supervisor_log', 'reporturi', 'reportfilteruri',
                                                                       'supervisor_permissionuri',
                                                                       '_ancestry', '_ecid', '_replication_position')
        },
        'useruri': rail.result('create_user')['uri'],
        'timeoffuri': item['uri'],
        'timeofftypename': item['name']
    }
