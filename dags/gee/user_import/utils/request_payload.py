# pylint: disable=too-many-statements
from pendulum import now
from rail import result, find_first_by_attr_and_get_attr, get_current_context, get_tenant_slug
from rail.lib.ecid import get_dagrun_ecid

def get_workweekhours_timezone_initial_schedule_location_uris(workweekhours=None, timezone=None, initial_schedule=None, location=None):
    if workweekhours:
        return find_first_by_attr_and_get_attr(result('get_all_custome_fields_dropdown_options'), 'displayText', workweekhours, 'uri')
    if timezone:
        return find_first_by_attr_and_get_attr(result('get_all_time_zones'), 'displayText', timezone, 'uri')
    if initial_schedule:
        return find_first_by_attr_and_get_attr(result('get_all_office_schedules'), 'displayText', initial_schedule, 'uri')
    if location:
        return find_first_by_attr_and_get_attr(result('get_enabled_locations'), 'displayText', location, 'uri')
    return None

def trigger_create_user_dag(item):
    supervisor_details = result('get_supervisor_details')['supervisordetails']
    user_custom_fields = result('get_user_custom_fields')
    company_department = result('get_company_department')
    return {
        'LoginName': item['loginname'],
        'FirstName': item['firstname'],
        'LastName': item['lastname'],
        'EmployeeType': item['employeetype'],
        'Department': item['department'],
        'Enabled': item['enabled'],
        'EmployeeId': item['employeeid'],
        'StartDate': item['startdate'],
        'EndDate': item['enddate'],
        'EmailAddress': item['emailaddress'],
        'SupervisorID': item['supervisorid'],
        'PermissionSets': item['permissionsets'],
        'Location': item['location'],
        'Timezone': item['timezone'],
        'Workweek': item['workweek'],
        'HolidayCalendar': item['holidaycalendar'],
        'InitialScheduleName': item['initialschedulename'],
        'AnnualSalary': item['annualsalary'].replace(",", ""),
        'ELT': item['elt'],
        'firstlinemanager': f"{supervisor_details['firstname']} {supervisor_details['lastname']}",
        'secondlinemanager': item['secondlinemanager'],
        'workweekhours': item['workweekhours'],
        'businesscardtitle': item['businesscardtitle'],
        'costcenter': item['costcenter'],
        'division': item['division'],
        'annualuri': user_custom_fields['annualsalary'],
        'elturi': user_custom_fields['elt'],
        'businesscardtitleuri': user_custom_fields['businesscardtitle'],
        'firstlineuri': user_custom_fields['firstlinemanger'],
        'secondlineuri': user_custom_fields['secondlinemanger'],
        'workweekuri': user_custom_fields['workweekhours'],
        'workweek_dropdown_valueuri': get_workweekhours_timezone_initial_schedule_location_uris(workweekhours=item['workweekhours']),
        'companydeparmenturi': company_department['uri'],
        'timezoneuri': get_workweekhours_timezone_initial_schedule_location_uris(timezone=item['timezone']),
        'officescheduleuri': get_workweekhours_timezone_initial_schedule_location_uris(initial_schedule=item['initialschedulename']),
        'supervisordetails': {
            "LoginName":supervisor_details['loginname'],
            "FirstName":supervisor_details['firstname'],
            "LastName":supervisor_details['lastname'],
            "EmployeeType":supervisor_details['employeetype'],
            "Department":supervisor_details['department'],
            "Enabled":supervisor_details['enabled'],
            "EmployeeId":supervisor_details['employeeid'],
            "StartDate":supervisor_details['startdate'],
            "EndDate":supervisor_details['enddate'],
            "EmailAddress":supervisor_details['emailaddress'],
            "SupervisorID":supervisor_details['supervisorid'],
            "PermissionSets":supervisor_details['permissionsets'],
            "Location":supervisor_details['location'],
            "Timezone":supervisor_details['timezone'],
            "Workweek":supervisor_details['workweek'],
            "HolidayCalendar":supervisor_details['holidaycalendar'],
            "InitialScheduleName":supervisor_details['initialschedulename'],
            "AnnualSalary":supervisor_details['annualsalary'],
            "ELT":supervisor_details['elt'],
            "firstlinemanager":None,
            "secondlinemanager":supervisor_details['secondlinemanager'],
            "workweekhours":supervisor_details['workweekhours'],
            "businesscardtitle":supervisor_details['businesscardtitle'],
            "costcenter":supervisor_details['costcenter'],
            "division":supervisor_details['division'],
            "annualuri":user_custom_fields['annualsalary'],
            "elturi":user_custom_fields['elt'],
            "businesscardtitleuri":user_custom_fields['businesscardtitle'],
            "firstlineuri":user_custom_fields['firstlinemanger'],
            "secondlineuri":user_custom_fields['secondlinemanger'],
            "workweekuri":user_custom_fields['workweekhours'],
            "timezoneuri":get_workweekhours_timezone_initial_schedule_location_uris(timezone=supervisor_details['timezone']),
            "sup_workweek_dropdown_valueuri":get_workweekhours_timezone_initial_schedule_location_uris(workweekhours=supervisor_details['workweekhours']),
            "officescheduleuri":get_workweekhours_timezone_initial_schedule_location_uris(initial_schedule=supervisor_details['initialschedulename'])
        },
        "locationuri": get_workweekhours_timezone_initial_schedule_location_uris(location=item['location']),
        'calling_dag_id': get_dagrun_ecid(get_current_context()['dag_run']),
        "gee_user_import_lookup_table": result('gee_user_import_logs'),
        "gee_supervisor_lookup_table": result('gee_supervisor_logs'),
    }

def trigger_update_user_dag(item):
    response = trigger_create_user_dag(item)
    response['useruri'] = result('user_check')['useruri']
    return response

def trigger_disable_user_dag(item):
    supervisor_details = result('get_supervisor_details')['supervisordetails']
    return {
        'LoginName': item['loginname'],
        'FirstName': item['firstname'],
        'LastName': item['lastname'],
        'EmployeeType': item['employeetype'],
        'Department': item['department'],
        'Enabled': item['enabled'],
        'EmployeeId': item['employeeid'],
        'StartDate': item['startdate'],
        'EndDate': item['enddate'],
        'EmailAddress': item['emailaddress'],
        'SupervisorID': item['supervisorid'],
        'PermissionSets': item['permissionsets'],
        'Location': item['location'],
        'Timezone': item['timezone'],
        'Workweek': item['workweek'],
        'HolidayCalendar': item['holidaycalendar'],
        'InitialScheduleName': item['initialschedulename'],
        'AnnualSalary': item['annualsalary'].replace(",", ""),
        'ELT': item['elt'],
        'firstlinemanager': f"{supervisor_details['firstname']} {supervisor_details['lastname']}",
        'secondlinemanager': item['secondlinemanager'],
        'workweekhours': item['workweekhours'],
        'businesscardtitle': item['businesscardtitle'],
        'costcenter': item['costcenter'],
        'division': item['division'],
        'useruri': result('user_check')['useruri'],
        'calling_dag_id': get_dagrun_ecid(get_current_context()['dag_run']),
        "gee_user_import_lookup_table": result('gee_user_import_logs'),
        "gee_supervisor_lookup_table": result('gee_supervisor_logs'),
    }

def get_create_user_payload(dag_run):
    return {
        "user": {
            "target": {
                "uri": None,
                "loginName": dag_run.conf['LoginName'],
                "parameterCorrelationId": None
            },
            "firstname": dag_run.conf['FirstName'],
            "lastname": dag_run.conf['LastName'],
            "emailAddress": dag_run.conf['EmailAddress'],
            "employeeId": dag_run.conf['EmployeeId'],
            "department": None,
            "supervisorAssignmentSchedule": None,
            "schedulePolicySchedule": [],
            "workWeekStartDayUri": None,
            "employmentDateRange": {
                "startDate": result('split_start_date'),
                "endDate": None,
                "relativeDateRangeUri": None,
                "relativeDateRangeAsOfDate": None
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['LoginName'],
                "SSOName": dag_run.conf['LoginName'],
                "password": None
            },
            "holidayCalendar": None,
            "timeOffPolicy": None,
            "permissionSets": [], 
            "policySets": [
                {
                    "uri": None,
                    "name": "Time Off"
                }
            ],
            "employeeType": None,
            "timesheetPeriodTypeUri": None,
            "costRateSchedule": None,
            "payrollRateSchedule": None,
            "defaultBillingRate": None,
            "timesheetApprovalPath": None,
            "expenseApprovalPath": None,
            "timeOffApprovalPath": None,
            "customFieldValues": [],
            "assignedActivities": [],
            "timeZone": None,
            "overtimeRuleAssignmentSchedule": None,
            "validationRuleAssignmentSchedule": None,
            "locationSchedule": [],
            "divisionSchedule": [],
            "costCenterSchedule": [],
            "serviceCenterSchedule": [],
            "departmentGroupSchedule": [],
            "employeeTypeGroupSchedule": [],
            "timesheetPeriodSchedule": [],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [],
            "displayNameParameter": None
        }
    }

def apply_user_modifications_emplyeetype(dag_run):
    return {
        "user": {
            "uri": result('create_user_in_replicon')['uri'],
            "loginName": None,
            "parameterCorrelationId": None
        },
        "modifications":{
        "employeeTypeGroupScheduleToApply": {
            "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementEmployeeTypeGroupSchedule": [],
            "updateEmployeeTypeGroupScheduleOverDateRange": {
                "replacementEmployeeTypeGroupScheduleEntries": [
                {
                    "employeeTypeGroup": {
                    "uri": None,
                    "parentUri": None,
                    "name": dag_run.conf['EmployeeType']
                    },
                    "effectiveDate": None,
                }
                ],
                "endDate": None
            }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def apply_user_modifications_holiday_calendar(dag_run):
    return {
        "user": {
            "uri": result('create_user_in_replicon')['uri'],
            "loginName": None,
            "parameterCorrelationId": None
        },
        "modifications":{
            "holidayCalendarToApply": {
            "holidayCalendar": {
                "uri": None,
                "name": dag_run.conf['HolidayCalendar']
            }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def apply_user_modifications_division(dag_run):
    return {
        "user": {
            "uri": result('create_user_in_replicon')['uri'],
            "loginName": None,
            "parameterCorrelationId": None
        },
        "modifications": {
            "divisionScheduleToApply": {
            "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
            "replacementDivisionSchedule": [
                {
                "division": {
                    "uri": None,
                    "parentUri": None,
                    "name": dag_run.conf['division']
                },
                "effectiveDate": None
                }
            ],
            "updateDivisionScheduleOverDateRange": None
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def apply_user_modifications_timezone_location(dag_run):
    return {
        "user": {
            "uri": result('create_user_in_replicon')['uri'],
            "loginName": None,
            "parameterCorrelationId": None
        },
        "modifications": {
            "timezoneToApply": {
            "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
            "timezone": {
                "uri": dag_run.conf['timezoneuri'],
                "IANAName": None
            }
            },
        "locationScheduleToApply": {
            "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
            "replacementLocationSchedule": [
                {
                "location": {
                    "uri": None,
                    "parentUri": None,
                    "name": dag_run.conf['Location']
                },
                "effectiveDate": None
                }
            ],
            "updateLocationScheduleOverDateRange": None
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_search_user_param(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:user-name"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                "text": dag_run.conf['SupervisorID']
                }
            }
        }
    }

def get_supervisordetails(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:user-name"
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:enabled"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['supervisorloginname']
                }
            }
        }
    }

def trigger_user_supervisor_dag(dag_run):
    return {
        'LoginName': dag_run.conf['LoginName'],
        'FirstName': dag_run.conf['FirstName'],
        'LastName': dag_run.conf['LastName'],
        'EmployeeType': dag_run.conf['EmployeeType'],
        'Department': dag_run.conf['Department'],
        'Enabled': dag_run.conf['Enabled'],
        'EmployeeId': dag_run.conf['EmployeeId'],
        'StartDate': dag_run.conf['StartDate'],
        'EndDate': dag_run.conf['EndDate'],
        'EmailAddress': dag_run.conf['EmailAddress'],
        'SupervisorID': dag_run.conf['SupervisorID'],
        'PermissionSets': dag_run.conf['PermissionSets'],
        'Location': dag_run.conf['Location'],
        'Timezone': dag_run.conf['Timezone'],
        'Workweek': dag_run.conf['Workweek'],
        'HolidayCalendar': dag_run.conf['HolidayCalendar'],
        'InitialScheduleName': dag_run.conf['InitialScheduleName'],
        'AnnualSalary': dag_run.conf['AnnualSalary'],
        'ELT': dag_run.conf['ELT'],
        'firstlinemanager': dag_run.conf['firstlinemanager'],
        'secondlinemanager': dag_run.conf['secondlinemanager'],
        'workweekhours': dag_run.conf['workweekhours'],
        'businesscardtitle': dag_run.conf['businesscardtitle'],
        'costcenter': dag_run.conf['costcenter'],
        'division': dag_run.conf['division'],
        'annualuri': dag_run.conf['annualuri'],
        'elturi': dag_run.conf['elturi'],
        'businesscardtitleuri': dag_run.conf['businesscardtitleuri'],
        'firstlineuri': dag_run.conf['firstlineuri'],
        'secondlineuri': dag_run.conf['secondlineuri'],
        'workweekuri': dag_run.conf['workweekuri'],
        'workweek_dropdown_valueuri': dag_run.conf['workweek_dropdown_valueuri'],
        'companydeparmenturi': dag_run.conf['companydeparmenturi'],
        'timezoneuri': dag_run.conf['timezoneuri'],
        'officescheduleuri': dag_run.conf['officescheduleuri'],
        'calling_dag_id': get_dagrun_ecid(get_current_context()['dag_run'])
    }

def get_today_date_format():
    today = now().strftime("%m/%d/%Y")
    return {
        "year": today.split('/')[2],
        "month": today.split('/')[0],
        "day": today.split('/')[1]
    }

def get_bulk_user_data(dag_run):
    return {
        "users": [
            {
            "uri": dag_run.conf['useruri']
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def update_userdata_9(dag_run):
    return {
        "user": {
        "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "policySetsToApply": {
            "policySetUrisToAssign": [
                "urn:replicon-tenant:"+get_tenant_slug()+":policy-set:a8dcc85a-09fb-4c84-9dae-d125a21b4e47"
            ],
            "policyUrisToRemovePolicySet": []
            },
            "activitiesToApply": [],
            "timeOffApprovalPathToApply": {
            "uri": "urn:replicon-tenant:"+get_tenant_slug()+":approval-path:7"
            },
            "customFieldValuesToApply": []
        }
    }

def update_user_loginname(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "securitySettingsToApply": {
                "loginEnabled": "true",
                "forcePasswordChange": "false",
                "loginName": dag_run.conf['LoginName'],
                "ssoName": dag_run.conf['LoginName'],
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "emailMFAResendVerificationEmail": "false",
                "emailMFATryAddMethodFromUsersEmail": "false",
                "isMFAMethodRequired": "false",
                "clearIsLockedOut": "false"
                }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_firstname_lastname(input_name, user_details_name):
    if input_name == user_details_name:
        return None
    return input_name

def get_email(input_email, user_details_email):
    if user_details_email and input_email == user_details_email:
        return user_details_email
    return input_email

def update_email_firstname_or_lastname(dag_run):
    user_details = result('get_user_data')[0]['userDetails']
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "userDetailsToApply": {
                "firstName":get_firstname_lastname(dag_run.conf['FirstName'], user_details['firstName']),
                "lastName":get_firstname_lastname(dag_run.conf['LastName'], user_details['lastName']),
                "emailAddress": {
                    "emailAddress": get_email(dag_run.conf['EmailAddress'], user_details['emailAddress'])
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def department_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "departmentGroupScheduleToApply": {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": {
                                "parent": {
                                    "uri": dag_run.conf['companydeparmenturi']
                                },
                                "name": dag_run.conf['Department']
                            },
                            "effectiveDate": get_today_date_format()
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def timezone_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timezoneToApply": {
            "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
            "timezone": {
                "uri": dag_run.conf['timezoneuri']
            }
            },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }
    }

def holiday_calendar_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications":{
            "holidayCalendarToApply": {
                "holidayCalendar": {
                    "name": dag_run.conf['HolidayCalendar']
                }
            },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }
    }

def location_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications":{
        "locationScheduleToApply": {
            "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementLocationSchedule": [],
            "updateLocationScheduleOverDateRange": {
                "replacementLocationScheduleEntries": [
                    {
                        "location": {
                            "name": dag_run.conf['Location']
                            },
                        "effectiveDate": get_today_date_format()
                    }
                ]
            }
            },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }
    }

def timesheet_period_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timesheetPeriodScheduleToApply": {
                "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementTimesheetPeriodSchedule": [],
                "updateTimesheetPeriodScheduleOverDateRange": {
                    "replacementTimesheetPeriodScheduleEntries": [
                        {

                            "timesheetPeriod": {
                                "uri":dag_run.conf['timesheetperioduri']
                            },
                            "effectiveDate": get_today_date_format()
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def division_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications":{
        "divisionScheduleToApply": {
            "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementDivisionSchedule": [],
            "updateDivisionScheduleOverDateRange": {
                "replacementDivisionScheduleEntries": [
                    {
                        "division": {
                        "name": dag_run.conf['division']
                        },
                        "effectiveDate": get_today_date_format()
                    }
                ]
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }
    }


def employee_type_update_payload(dag_run):
    return {
        "user": {
                "uri": dag_run.conf['useruri']
        },
        "modifications":{
            "employeeTypeGroupScheduleToApply": {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {
                            "name": dag_run.conf['EmployeeType']
                            },
                            "effectiveDate": get_today_date_format()
                        }
                    ]
                }
            },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }
    }


def office_schedule_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "schedulePolicyToApply": {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementSchedule": [],
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                        {
                            "schedulePolicy": {
                            "officeSchedule": {
                                "officeScheduleUri": dag_run.conf['officescheduleuri'],
                                "name": dag_run.conf['InitialScheduleName']
                            },
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": get_today_date_format()
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_user_details_from_supervisorid_54(dag_run):
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
                    'text': dag_run.conf['SupervisorID']
                }
            }
        }
    }

def get_permissionsets_78():
    permissionsets = result('get_user_data')[0]['permissionSets']
    return [{"value": permissionset['name']} for permissionset in permissionsets]

def get_permissionsets_80(dag_run):
    permissionsets = dag_run.conf['PermissionSets'].split('|')
    return [{"value": permissionset.strip()} for permissionset in permissionsets]

def get_user_details_from_supervisorid_93(dag_run):
    return {
        'page': '1',
        'pagesize': '100',
        'columnUris': [
            'urn:replicon:user-list-column:user',
            'urn:replicon:user-list-column:employee-id',
            'urn:replicon:user-list-column:login-name'
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {
                    'text': dag_run.conf['SupervisorID']
                }
            }
        }
    }

def get_user_details_from_supervisorid_95(dag_run):
    return {
        'page': '1',
        'pagesize': '100',
        'columnUris': [
            'urn:replicon:user-list-column:supervisor',
            'urn:replicon:user-list-column:user'
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': 'urn:replicon:user-list-filter:user'
            },
            'operatorUri': 'urn:replicon:filter-operator:equal',
            'rightExpression': {
                'value': {
                    'text': dag_run.conf['useruri']
                }
            }
        }
    }

def get_supervisor_conf_payload(item):
    permissionsets = result('get_all_permission_sets')
    return {
        'loginname': item['properties']['userloginname'],
        'username': item['properties']['username'],
        'supervisorloginname': item['properties']['supervisorloginname'],
        'parentjobid': item['properties']['jobid'],
        'childjobid': item['properties']['childjobid'],
        'useruri': item['properties']['useruri'],
        'action': item['properties']['action'],
        'employeeid': item['properties']['empid'],
        'supervisorpermissionuri': find_first_by_attr_and_get_attr(permissionsets, "displayText", "Supervisor", "uri"),
        'enduserpermissionformanager': find_first_by_attr_and_get_attr(permissionsets, "displayText", "Project Resource", "uri"),
        'calling_dag_id': get_dagrun_ecid(get_current_context()['dag_run']),
    }
