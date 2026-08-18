from datetime import datetime
from math import ceil, floor
import pytz
from pendulum import now
import rail
from pwcglobal.user_import_v6.config import timeoff_approval_path_mapper
from pwcglobal.user_import_v6.config import language_mapper
from pwcglobal.user_import_v6.config import work_compliance_policy_mapper
null = None
true = True
false = False

# pylint: disable=too-many-return-statements


def get_attr_value(data, attr_path, default_value=None):
    if not data or not attr_path:
        return default_value

    cur_attr_path = attr_path.split(
        '.')[0] if '.' in attr_path else attr_path
    child_attr_path = '.'.join(attr_path.split('.')[1:]) if '.' in attr_path and len(
        attr_path.split('.')) > 1 else None
    if not cur_attr_path:
        return default_value

    if isinstance(data, dict) and cur_attr_path in data:
        if not child_attr_path:
            return data[cur_attr_path]
        return get_attr_value(data[cur_attr_path], child_attr_path, default_value)

    if cur_attr_path.isnumeric() and isinstance(data, list) and int(cur_attr_path) < len(data):
        if not child_attr_path:
            return data[int(cur_attr_path)]
        return get_attr_value(data[int(cur_attr_path)], child_attr_path, default_value)
    return default_value


def get_conf():
    return rail.get_current_context()['dag_run'].conf


def get_user_uri():
    return get_conf()['useruri']


def get_created_user_uri():
    return rail.result("create_user")['uri']


def get_user_uri_template_exp():
    return '{{ dag_run.conf.useruri }}'


def get_today_date():
    today_date = datetime.now(pytz.timezone('Europe/London'))
    return {
        'year': today_date.year,
        'month': today_date.month,
        'day': today_date.day
    }


def get_replicon_date(date_str):
    if not date_str:
        return None
    # date format in 20060401
    try:
        date = datetime.strptime(date_str, '%Y%m%d')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def get_division_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:code",
            "urn:replicon:division-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:division-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_dept_group_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:code",
            "urn:replicon:department-group-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:department-group-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_cost_center_group_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center",
            "urn:replicon:cost-center-list-column:code",
            "urn:replicon:cost-center-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:cost-center-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_toil_time_off_uri(country,config):
    if country:
        country = "PwC " + country
        if country in config.toil_timeoff_mapper:
            time_off_type = config.toil_timeoff_mapper[country]
            return rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeofftypes'), 'displayText', time_off_type, 'uri')
    return null


def get_conf_uris():
    return {
        'timeoffpolicyuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'), 'name', 'Time Off', 'uri'),
        'customfielduri': {
            'prefix': rail.find_first_by_attr_and_get_attr(rail.result('get_all_customfields'), 'displayText', 'Prefix', 'uri'),
            'homelocation': rail.find_first_by_attr_and_get_attr(rail.result('get_all_customfields'), 'displayText', 'Home office location', 'uri'),
            'grade': rail.find_first_by_attr_and_get_attr(rail.result('get_all_customfields'), 'displayText', 'Grade', 'uri'),
            'resourcerole': rail.find_first_by_attr_and_get_attr(rail.result('get_all_customfields'), 'displayText', 'Resource Role', 'uri'),
            'workdayid': rail.find_first_by_attr_and_get_attr(rail.result('get_all_customfields'), 'displayText', 'Workday ID', 'uri'),
            'partyid': rail.find_first_by_attr_and_get_attr(rail.result('get_all_customfields'), 'displayText', 'Party ID', 'uri'),
            'profilestatus': rail.find_first_by_attr_and_get_attr(rail.result('get_all_customfields'), 'displayText', 'Profile Status', 'uri'),
            'loscode': rail.find_first_by_attr_and_get_attr(rail.result('get_all_customfields'), 'displayText', 'LoS Code', 'uri'),
            'toil': rail.find_first_by_attr_and_get_attr(rail.result('get_all_customfields'), 'displayText', 'TOIL', 'uri'),
            "ftepercenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_customfields'), 'displayText', 'FTE Percent', 'uri'),
            "linemanager": rail.find_first_by_attr_and_get_attr(rail.result('get_all_customfields'), 'displayText', 'Line Manager', 'uri'),
        },
        'timeofftypeuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeofftypes'), 'displayText', 'Public Holidays', 'uri'),
        'managerpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permissionset'), 'displayText', 'Matrix/Team Manager', 'uri')
    }


def get_timeoff_approval_path_from_mapper(user_country):
    timeoff_approval_path_from_mapper = next(iter(filter(
        lambda x: x['Country'] == user_country, timeoff_approval_path_mapper)), {}).get('Time_Off_Approval_Path', null)
    return timeoff_approval_path_from_mapper


def get_work_compliance_policy_from_mapper(user_country, user_company_code):
    company_code_fullpath = rail.find_first_by_attr_and_get_attr(rail.result('get_dept_group'), 'code', (user_company_code.split('|')[-1] if (
        user_company_code and '|' in user_company_code) else user_company_code), 'fullpath')
    level_3_in_company_code_fullpath = company_code_fullpath[2] if company_code_fullpath and len(
        company_code_fullpath) > 2 else null
    matching_value_in_mapper = next(iter(filter(lambda x: x['Country'] == user_country and
                                                x['Company Code'].lower() == level_3_in_company_code_fullpath.lower(), work_compliance_policy_mapper)), {}).get(
                                                    'Work Compliance Policy', null) if level_3_in_company_code_fullpath else null
    return matching_value_in_mapper


def get_process_user_conf(item,config):
    item['ScheduleType'] = "0|8|8|8|8|8|0" if not item['ScheduleType'] else item['ScheduleType']
    return {
        **{k.lower(): v for k, v in item.items()},
        **rail.result('get_conf_uris'),
        **{
            'employeetypegroupuri': rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_emp_groups'), 'displayText', item['EmployeeType'], 'uri'),
            'companycodegroupuri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_dept_group'), 'code', item['CompanyCode'].split('|')[-1] if item['CompanyCode']
                and '|' in item['CompanyCode'] else item['CompanyCode'], 'uri'),
            'legalentitygroupuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_division'), 'code', item['LegalEntity'], 'uri'),
            'countriesgroupuri': rail.find_first_by_attr_and_get_attr(rail.result('get_updated_locations'), 'displayText', item['Country'], 'uri'),
            'timesheettemplateuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'), 'displayText', item['TimeSheetTemplate'], 'uri'),
            'timesheetapprovalpathuri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_approval_paths'), 'displayText', item['TimeSheetApprovalPath'], 'uri'),
            'holidaycalenderuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calendars'), 'displayText', item['HolidayCalendar'], 'uri'),
            'timezoneuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_timezones'), 'ianaName', item['TimeZone'], 'uri'),
            'supervisorlegalentityuri': rail.find_first_by_attr_and_get_attr(
                rail.result(
                    'get_all_division'), 'code', item['Supervisor'].split('|')[-1]
                if item['Supervisor'] and '|' in item['Supervisor'] else item['Supervisor'], 'uri'),
            'scheduleuri': rail.find_first_by_attr_and_get_attr(rail.result('get_updated_schedules'), 'displayText', item['ScheduleType'], 'uri'),
            'gradedropdownuri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_grade_options'), 'displayText', item['Grade'], 'uri'),
            'profilestatusdropdownuri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_profilestatus_options'), 'displayText', item['ProfileStatus'], 'uri'),
            'permissionseturi': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permissionset'), 'displayText', item['AddUserPermission'] or 'End User', 'uri'),
            'has_loaded_report_users': bool(rail.result('load_all_report_users')),
            'useruri': rail.find_first_by_attr_and_get_attr(rail.result('load_all_report_users') or [], 'Login_Name', item['LoginName'], 'useruri'),
            "toiltimeofftypeuri": get_toil_time_off_uri(item["Country"],config),
            "toildropdownuri": rail.find_first_by_attr_and_get_attr(
                rail.result('get_toil_options'), 'displayText', item['TOIL'], 'uri'),
            "linemanagerlegalentityuri": rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_division'),
                'code', item['LineManagerLegalEntitypartyID'],
                'uri') if item['LineManagerLegalEntitypartyID'] else "",
            "supervisorpermissionseturi": rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permissionset'), 'displayText', "Supervisor", 'uri'),
            "payruleuri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_payrule_scripts"), "displayText", item["Payrule"], "uri"),
            "timeentryapprovalpathuri": rail.find_first_by_attr_and_get_attr(
                rail.result('get_time_entry_approval_paths'), 'displayText', item['TimeEntryApprovalPath'], 'uri'),
            "systemapprovalpathuri": rail.find_first_by_attr_and_get_attr(
                rail.result('get_time_entry_approval_paths'), 'displayText', "System Approval", 'uri'),
            'ftepercent': ceil(float(item['FTEPercent'])) if item["FTEPercent"] and float(item['FTEPercent']) % 1 >= 0.5
            else floor(float(item['FTEPercent'])) if item["FTEPercent"] else null,
            "zerotimeuserpermissionseturi": rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permissionset'), 'displayText', config.zt_permission_mapper[item["Country"]], 'uri')
            if item["Country"] in config.zt_permission_mapper else null,
            "zerotime_mapper": config.zt_permission_mapper,
            "zerotimepermission": config.zt_permission_mapper[item["Country"]]
            if item["Country"] in config.zt_permission_mapper else null,
            "timeoffapprovalpath": get_timeoff_approval_path_from_mapper(item["Country"]),
            "timeoffapprovalpathuri": rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_timeoff_approval_paths'), 'displayText', get_timeoff_approval_path_from_mapper(item["Country"]), 'uri') if bool(
                    get_timeoff_approval_path_from_mapper(item["Country"])) else null,
            "language_if_not_default": next(iter(filter(
                lambda x: x['Country'] == item["Country"], language_mapper)), {}).get('Language', null),
            "language_uri_if_not_default": next(iter(filter(
                lambda x: x['Country'] == item["Country"], language_mapper)), {}).get('Language_URI', null),
            "work_compliance_policy": get_work_compliance_policy_from_mapper(item["Country"], item['CompanyCode']),
            "supervisory_org_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_replicon_cost_centers'), 'name', item['SupervisoryOrgName'], 'uri')
        },
    }


def get_search_user_param():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
                "urn:replicon:user-list-column:login-name",
                "urn:replicon:user-list-column:user"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
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
                    "text": "{{ dag_run.conf.loginname }}",
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_timesheet_periodtype():
    return {
        "user": {
            "uri": get_user_uri(),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "timesheetPeriodScheduleToApply": {
                "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                "replacementTimesheetPeriodSchedule": [
                    {
                        "timesheetPeriod": {
                            "uri": null,
                            "name":  get_conf()['timesheetperiodtype'] or "Weekly without crossing starting Sunday",
                        },
                        "effectiveDate": get_replicon_date(get_conf()['startdate']),
                    }
                ],
                "updateTimesheetPeriodScheduleOverDateRange": null
            },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }
    }


def get_emptype_update_param():
    return {
        "user": {
            "uri": get_user_uri(),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "employeeTypeGroupScheduleToApply": {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {
                                "uri": get_conf()['employeetypegroupuri'],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": get_today_date()
                        }
                    ],
                    "endDate": null
                }
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_update_company_code_param():
    return {
        "user": {
            "uri": get_user_uri(),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "departmentGroupScheduleToApply": {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": {
                                "uri": get_conf()['companycodegroupuri'],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": get_today_date()
                        }
                    ],
                    "endDate": null
                }
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_update_supervisory_org_param():
    return {
        "user": {
            "uri": get_user_uri(),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementCostCenterSchedule": [],
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {
                                "uri": get_conf()['supervisory_org_uri'],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": get_today_date()
                        }
                    ],
                    "endDate": null
                }
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_update_workcomplaincepolicy_param():
    return {
        "user": {
            "uri": get_user_uri(),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "workCompliancePolicyAssignmentScheduleToApply": {
                "userWorkCompliancePolicyAssignmentScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementWorkCompliancePolicyAssignmentSchedule": [],
                "updateWorkCompliancePolicyAssignmentScheduleOverDateRange": {
                    "replacementWorkCompliancePolicyAssignmentScheduleEntries": [
                        {
                            "workCompliancePolicy": {
                                "uri": null,
                                "name": get_conf()['work_compliance_policy']
                            },
                            "effectiveDate": get_today_date()
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_update_businessunit_param():
    return {
        "user": {
            "uri": get_user_uri(),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "divisionScheduleToApply": {
                "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDivisionSchedule": [],
                "updateDivisionScheduleOverDateRange": {
                    "replacementDivisionScheduleEntries": [
                        {
                            "division": {
                                "uri":  get_conf()['legalentitygroupuri'],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": get_today_date()
                        }
                    ],
                    "endDate": null
                }
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_udpate_country_param():
    return {
        "user": {
            "uri": get_user_uri(),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "locationScheduleToApply": {
                "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementLocationSchedule": [],
                "updateLocationScheduleOverDateRange": {
                    "replacementLocationScheduleEntries": [
                        {
                            "location": {
                                "uri":  get_conf()['countriesgroupuri'],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": get_today_date()
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_put_userpolicy_datascope_param():
    return {
        "userUri": get_user_uri() if get_user_uri() else get_created_user_uri(),
        "policyDataAccessScopes": [
            {
                "policyUri": "urn:replicon:policy:user",
                "locations": [
                    {
                        "location": {
                            "uri": get_conf()['countriesgroupuri'],
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "groupSpecificationModeUri": null,
                        "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:include-descendants"
                    }
                ],
                "divisions": [],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [],
                "employeeTypeGroups": []
            }
        ]
    }


def get_user_notification_preference():
    return {
        "user": {
            "uri": get_created_user_uri(),
        },
        "preferences": {
            "notificationDeliveryPreferences": [
                {
                    "objectTypeUri": "urn:replicon:object-type:project",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:user",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:timesheet",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:time-off",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:holiday",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                }
            ],
            "sharedDeliveryPreferenceOptionUris": [
                "urn:replicon:user-shared-delivery-preference-option:do-not-deliver-on-time-off",
                "urn:replicon:user-shared-delivery-preference-option:do-not-deliver-on-non-work-days"
            ]
        }
    }


def get_put_timeoff_policy_datascope_param():
    return {
        "userUri": get_user_uri() if get_user_uri() else get_created_user_uri(),
        "policyDataAccessScopes": [
            {
                "policyUri": "urn:replicon:policy:time-off",
                "locations": [
                    {
                        "location": {
                            "uri": get_conf()['countriesgroupuri'],
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "groupSpecificationModeUri": null,
                        "groupDescendantModeUri": null
                    }
                ],
                "divisions": [],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [],
                "employeeTypeGroups": []
            }
        ]
    }


def get_update_displayname_param():
    return {
        "user": {
            "uri": get_user_uri(),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "userDetailsToApply": {
                "displayNameParameter": {

                    "displayName": get_display_name()
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_timesheet_schedule_param():
    schedules = list(map(
        lambda x: {
            "timesheetPeriod": {
                "uri": x['timesheetPeriod']['uri'],
                "name": null
            },
            "effectiveDate": x['effectiveDate']
        },
        filter(lambda x: not x["effectiveDate"] or get_date_from_replicon_date(
            x["effectiveDate"]) != get_date_from_replicon_date(get_today_date()), rail.result('get_current_timesheetperiod_schedule'))))

    schedules.append({
        "timesheetPeriod": {
            "uri": rail.result('get_timesheetperiodtype_uri')['uri'],
            "name": null
        },
        "effectiveDate": get_today_date(),
    })
    return {
        "userUri": get_user_uri(),
        "scheduleEntries": schedules
    }


def get_current_schedule(data):
    if not data and len(data) == 0:
        return None
    current_schedule = list(filter(lambda x: datetime(
        **x['effectiveDate']) if x['effectiveDate'] else datetime.min <= datetime(**get_today_date()), data))
    return None if len(current_schedule) == 0 else current_schedule[-1]


def get_search_user_by_partyid_param():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
                "urn:replicon:user-list-column:user",
                "urn:replicon:user-list-column:employee-id",
                "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
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
                    "text": get_conf()['supervisor'].split('||')[0],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_search_user_by_legalentity_param():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:division",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:employee-id"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
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
                        "text": get_conf()['supervisor'].split('||')[0],
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
                    "filterDefinitionUri": "urn:replicon:user-list-filter:division"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": get_conf()['supervisorlegalentityuri'],
                        "uris": [],
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
                "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": "true",
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
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
        }
    }


def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])


def get_office_schedule_param():
    schedules = list(map(
        lambda x: {
            "effectiveDate": x['effectiveDate'],
            "schedulePolicy": {
                "officeScheduleUri": x['officeSchedule']['uri'],
                "name": null,
                "officeSchedule": {
                    "officeScheduleUri": x['officeSchedule']['uri'],
                    "name": null
                },
                "scheduleTypeUri": x['scheduleTypeUri'],
            }
        },
        filter(lambda x: not x["effectiveDate"] or get_date_from_replicon_date(
            x["effectiveDate"]) != get_date_from_replicon_date(get_today_date()), rail.result('bulk_get_user3')['schedulePolicies'])))

    schedules.append({
        "effectiveDate": get_today_date(),
        "schedulePolicy": {
            "officeScheduleUri": null,
            "name": get_conf()['scheduletype'],
            "officeSchedule": {
                "officeScheduleUri": null,
                "name": get_conf()['scheduletype']
            },
            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
        },
    })
    return {
        "userUri": get_user_uri(),
        "scheduleEntries": schedules
    }


def get_put_column_settings_for_user_team_tab_data(user_uri):
    return {
        "userUri": user_uri,
        "listId": "myTeamTimeSheet_list",
        "columnSettings": [
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": false,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 0,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet-owner",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 220,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet-status",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 220,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet-period",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 190,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-info",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 0,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-warning",
                "settings": [
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 0,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-error",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 170,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:total-working-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 100,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:time-off-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 100,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:total-payable-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 100
                        }
                    }
                ]
            }
        ]
    }


def get_put_column_settings_for_user_timesheet_tab_data(user_uri):
    return {
        "userUri": user_uri,
        "listId": "timesheet_list",
        "columnSettings": [
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": false,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 0,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet-period",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 220,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet-status",
                "settings": [
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 220,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:total-working-duration",
                "settings": [
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 100,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:time-off-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 100
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:total-payable-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 100,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            }
        ]
    }


def get_put_column_settings_for_user_approvals_data(user_uri):
    return {
        "userUri": user_uri,
        "listId": "timeSheetForApproval_list",
        "columnSettings": [
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": false,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 0,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet-owner",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 220,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet-period",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 190,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-info",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 0,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-warning",
                "settings": [
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 0,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-error",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 170,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:total-working-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 100,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:time-off-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 100
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:total-payable-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": true,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "uri": null,
                            "slug": null,
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": 100,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null
                        }
                    }
                ]
            }
        ]
    }


def get_search_user_by_empid_status_country_param():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:location"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
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
                        "text": get_conf()['employeeid'],
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
                        "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": "true",
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
                            "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                        },
                        "operatorUri": "urn:replicon:filter-operator:text-search",
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
                                "text": get_conf()['firstname'],
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
                            "filterDefinitionUri": "urn:replicon:user-list-filter:location"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": get_conf()['countriesgroupuri'],
                                "uris": [],
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
        }
    }


def get_schedule_param():
    return [
        {
            "schedulePolicy": {
                "officeScheduleUri": null,
                "name": get_conf()['scheduletype'],
                "officeSchedule": {
                    "officeScheduleUri": null,
                    "name": get_conf()['scheduletype'],
                },
                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
            },
            "effectiveDate": null
        }
    ] if get_conf()['scheduletype'] else null


def get_holiday_calendar():
    return {
        "uri": get_conf()['holidaycalenderuri'],
        "name": null
    } if get_conf()['holidaycalenderuri'] else null


def get_policy_sets(config):
    policy_sets = []

    timeoff_policy = next(filter(lambda x: x['Country'] == 'Global' and x['Type'] == 'Time off Template'
                                 and x['Identifier'] == get_conf()['country'] and x['Value'], config.general_mapper), None)
    # if timeoff policy not present in general mapper, then add timeoff to policy sets
    if not timeoff_policy:
        policy_sets.append({
            'uri': null,
            'name': 'Time Off'
        })

    timesheet_policy = next(filter(lambda x: x['Country'] == 'Europe' and
                                   x['timesheet'] == get_conf()['timesheettemplate'] and
                                   x['policy'],
                                   config.timesheet_policy_mapper), {}).get('policy', None)
    if timesheet_policy:
        policy_sets.append({
            'uri': null,
            'name': timesheet_policy
        })

    if get_conf()['timesheettemplateuri']:
        policy_sets.append({
            'uri': get_conf()['timesheettemplateuri'],
            'name': null
        })

    return policy_sets


def get_timesheet_path():
    uri = get_conf()['timesheetapprovalpathuri']
    return {
        "uri": uri,
        "name": "System Approval" if not get_conf()['timesheetapprovalpath'] or not uri else null
    }


def get_time_entry_path():
    uri = get_conf()["timeentryapprovalpathuri"]
    if not uri or not get_conf()['timeentryapprovalpath']:
        uri = get_conf()["systemapprovalpathuri"]
    return {
        "userUri": rail.result("create_user")["uri"],
        "approvalPathUri": uri
    }


def get_custom_field_values():
    conf = get_conf()
    customfielduri = conf['customfielduri']
    custom_fields = []
    if conf['workdayid'] and customfielduri['workdayid']:
        custom_fields.append({
            'customField': {
                'uri': customfielduri['workdayid'],
            },
            'text': conf['workdayid'],
        })

    if conf['employeeid'] and customfielduri['partyid']:
        custom_fields.append({
            'customField': {
                'uri': customfielduri['partyid'],
            },
            'text': conf['employeeid'],
        })

    if conf['gradedropdownuri'] and customfielduri['grade']:
        custom_fields.append({
            'customField': {
                'uri': customfielduri['grade'],
            },
            'dropDownOption': {"uri": conf['gradedropdownuri']},
        })

    if conf['prefix'] and customfielduri['prefix']:
        custom_fields.append({
            'customField': {
                'uri': customfielduri['prefix'],
            },
            'text': conf['prefix'],
        })

    if conf['homeofficelocation'] and customfielduri['homelocation']:
        custom_fields.append({
            'customField': {
                'uri': customfielduri['homelocation'],
            },
            'text': conf['homeofficelocation'],
        })

    if conf['resourcerole'] and customfielduri['resourcerole']:
        custom_fields.append({
            'customField': {
                'uri': customfielduri['resourcerole'],
            },
            'text': conf['resourcerole'],
        })

    if conf['loscode'] and customfielduri['loscode']:
        custom_fields.append({
            'customField': {
                'uri': customfielduri['loscode'],
            },
            'text': conf['loscode'],
        })
    if conf['toildropdownuri'] and customfielduri['toil'] and conf['toiltimeofftypeuri']:
        custom_fields.append({
            'customField': {
                'uri': customfielduri['toil'],
            },
            'dropDownOption': {"uri": conf['toildropdownuri']},
        })
    if conf['profilestatusdropdownuri'] and customfielduri['profilestatus']:
        custom_fields.append({
            'customField': {
                'uri': customfielduri['profilestatus'],
            },
            'dropDownOption': {"uri": conf['profilestatusdropdownuri']},
        })

    return custom_fields


def get_timezone():
    return {
        "uri": get_conf()['timezoneuri'] or "urn:replicon:time-zone:europe-warsaw",
        "IANAName": null
    }


def get_location_schedule():
    return [
        {
            "location": {
                "uri": get_conf()['countriesgroupuri'],
                "parent": null,
                "name": null,
                "parameterCorrelationId": null
            },
            "effectiveDate": null
        }
    ] if get_conf()['countriesgroupuri'] else null


def get_division_schedule():
    return [
        {
            "division": {
                "uri": get_conf()['legalentitygroupuri'],
                "parent": null,
                "name": null,
                "parameterCorrelationId": null
            },
            "effectiveDate": null
        }
    ] if get_conf()['legalentitygroupuri'] else null


def get_costcenter_schedule():
    return [
        {
            "costCenter": {
                "uri": get_conf()['supervisory_org_uri'],
                "parentUri": null,
                "name": null
            },
            "effectiveDate": null
        }
    ] if get_conf()['supervisory_org_uri'] else null


def get_dept_group_schedule():
    if get_conf()['companycodegroupuri']:
        return [
            {
                "departmentGroup": {
                    "uri": get_conf()['companycodegroupuri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }
        ]
    return null


def get_emp_group_schedule():
    if get_conf()['employeetypegroupuri']:
        return [
            {
                "employeeTypeGroup": {
                    "uri": get_conf()['employeetypegroupuri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }
        ]
    return null


def get_timesheet_period_schedule():
    timesheetperiodtype_uri = (rail.result(
        'get_timesheetperiodtype_uri') or {}).get('uri', null)
    return [
        {
            "timesheetPeriod": {
                "uri": timesheetperiodtype_uri,
                "name": "Weekly without crossing starting Sunday" if not timesheetperiodtype_uri else null,
            },
            "effectiveDate": get_replicon_date(get_conf()['startdate'])
        }
    ]

# pylint: disable=unnecessary-lambda


def get_display_name():
    # 'Esmae Nxxx Gxxx,esmae.xxx.nxxxx.gxxxx@xxx.com (NL, Tax)'
    conf = get_conf()
    code = rail.result('get_location_details')['code'] if rail.result(
        'get_location_details') and bool(rail.result('get_location_details')['code']) else ''
    los_code = conf['loscode'] if bool(conf['loscode']) else ''
    los_code = f", {los_code}" if code and los_code else los_code
    display_name = f"{conf['firstname'] or ''};{conf['prefix'] or ''};{conf['lastname'] or ''};,{conf['emailaddress'] or ''};({code};{los_code})"
    display_name = ' '.join(filter(lambda x: bool(x), display_name.split(";"))).replace(
        " ,", ",").replace(" )", ")").replace("( ", "(")
    return display_name


def get_work_week(config):
    return next(filter(
        lambda x: x['Country'] == 'Global' and
        x['Type'] == 'workweek' and
        x['Identifier'] == get_conf()['workweek'] and
        x['Value'],
        config.general_mapper), {}).get('Value', 'urn:replicon:day-of-week:sunday')


def get_timeoff_approval_path():
    if get_conf()['timeoffapprovalpathuri']:
        return {
            "uri": get_conf()['timeoffapprovalpathuri'],
            "name": null
        }
    return null


def get_work_compliance_policy_assignment_schedule():
    if get_conf()['work_compliance_policy']:
        return [{
            "workCompliancePolicy": {
                "uri": null,
                "name": get_conf()['work_compliance_policy']
            },
            "effectiveDate": null
        }]
    return null


def get_create_user_data(config):
    conf = get_conf()
    permission_sets = [{
        "uri": conf['permissionseturi'],
        "name": null
    }]
    if conf["zerotimeuserpermissionseturi"]:
        permission_sets.append({
            "uri": conf['zerotimeuserpermissionseturi'],
            "name": null
        })

    validationlog = list(
        map(lambda x: x['field_name'], conf['validationlog'] or []))
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": conf['loginname'],
                "parameterCorrelationId": null
            },
            "firstname": conf['firstname'],
            "lastname": conf['lastname'],
            "emailAddress": conf['emailaddress'] if (config.company_key.lower() == 'pwc' and conf['emailaddress'] and 'emailaddress' not in validationlog) else null,
            "employeeId": conf['employeeid'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": get_schedule_param(),
            "workWeekStartDayUri": get_work_week(config),
            "employmentDateRange": {
                "startDate": get_replicon_date(conf['startdate']),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": conf['isloginenabled'] == 'Yes',
                "loginName": conf['loginname'],
                "SSOName": conf['loginname'],
                "password": null
            },
            "holidayCalendar": get_holiday_calendar(),
            "timeOffPolicy": null,
            "permissionSets": permission_sets,
            "policySets": get_policy_sets(config),
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": get_timesheet_path(),
            "expenseApprovalPath": null,
            "timeOffApprovalPath": get_timeoff_approval_path(),
            "customFieldValues": get_custom_field_values(),
            "assignedActivities": [],
            "timeZone": get_timezone(),
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": get_location_schedule(),
            "divisionSchedule": get_division_schedule(),
            "costCenterSchedule": get_costcenter_schedule(),
            "serviceCenterSchedule": null,
            "departmentGroupSchedule": get_dept_group_schedule(),
            "employeeTypeGroupSchedule": get_emp_group_schedule(),
            "timesheetPeriodSchedule": get_timesheet_period_schedule(),
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [],
            "displayNameParameter": {
                "displayName": get_display_name(),
            },
            "workCompliancePolicyAssignmentSchedule": get_work_compliance_policy_assignment_schedule()
        }
    }


def line_manager_request():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:division",
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-id"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                "bool": "true"
                }
            }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                "text": get_conf()["linemanagerpartyid"]
                }
            }
            }
        }
    }


def get_update_payrule_request():
    previous_payrule_schedule = list(map(lambda i: {
        "effectiveDate": i["effectiveDate"],
        "payRuleScript": {
            "uri": i["payRuleScript"]["uri"]
        }
    }, rail.result("bulk_get_user3")["payRuleScriptSchedule"]
    ))

    return {
        "userUri": get_conf()["useruri"],
        "scheduleEntries": [
            {
                "effectiveDate": rail.result("get_current_timesheet_period_payrule"),
                "payRuleScript": {
                    "uri": get_conf()["payruleuri"]
                }
            }
        ] + previous_payrule_schedule
    }


def check_past_date():
    today_d = now(tz='Europe/London')
    t_month = today_d.month
    if t_month < 10:
        t_month = "0" + str(t_month)
    today_date = datetime.strptime(
        str(today_d.year)+str(t_month)+str(today_d.day), "%Y%m%d")
    if get_conf()["ftepercenteffectivedate"]:
        return (today_date-datetime.strptime(get_conf()["ftepercenteffectivedate"], "%Y%m%d")).days
    return null


def get_ftevalue_json_request():
    fte_percent = str(get_conf()["ftepercent"])
    effectivedate = ""
    effectivedate = datetime.strftime(datetime.strptime(
        get_conf()["startdate"], "%Y%m%d"), "%d/%m/%Y")
    return [{
        "value": fte_percent,
        "effectivedate": effectivedate
    }]


def get_timesheet_start_date():
    timesheetstartdate = rail.result(
        "get_current_timesheet_period") or rail.result("get_past_timesheet_period")
    if timesheetstartdate["month"] < 10:
        timesheetstartdate["month"] = "0" + str(timesheetstartdate["month"])
    timesheetstartdatestr = str(timesheetstartdate["year"]) + \
        str(timesheetstartdate["month"])+str(timesheetstartdate["day"])
    return timesheetstartdatestr


def get_ftevalue_update_json_request():
    effectivedate = ""
    effectivedate = datetime.strftime(datetime.strptime(
        get_timesheet_start_date(), "%Y%m%d"), "%d/%m/%Y")
    return [{
        "value": str(get_conf()["ftepercent"]),
        "effectivedate": effectivedate
    }]


def get_existing_records():
    existing_records = rail.load_all_records(
        rail.result("query_unique_existing_records"))
    if existing_records:
        return list(map(lambda i: {
            "value": i["value"],
            "effectivedate": i["effectivedate"]
        }, existing_records))
    return []


def get_ftevalue_blob_update_json_request():
    fte = []
    fte = get_existing_records()

    new_records = rail.load_all_records(rail.result("query_new_records")) or\
        rail.load_all_records(rail.result("query_blob_fte_date"))
    if new_records:
        fte.extend(list(map(lambda i: {
            "value": i["value"],
            "effectivedate": i["effectivedate"]
        }, new_records)))
    return fte


def get_update_time_off_req(config):
    time_off_types = []
    time_off_types.extend(rail.result("get_all_time_off_types_for_user"))
    if get_conf()['toiltimeofftypeuri'] and \
            get_conf()['toiltimeofftypeuri'] not in time_off_types and get_conf()["toil"] == "Y":
        time_off_types.append(get_conf()['toiltimeofftypeuri'])
    elif get_toil_time_off_uri(rail.result("get_user_location_uri"),config) not in time_off_types and get_conf()["toil"] == "Y" and\
            get_toil_time_off_uri(rail.result("get_user_location_uri"),config):
        time_off_types.append(get_toil_time_off_uri(
            rail.result("get_user_location_uri"),config))
    return {
        "userUri": get_conf()["useruri"],
        "timeOffTypeUris": time_off_types
    }


def get_current_workcomplianceassignmentpolicy(user_workcomplianceassignmentpolicy_schedule):
    if not (user_workcomplianceassignmentpolicy_schedule):
        return null

    current_policy_details = user_workcomplianceassignmentpolicy_schedule[-1]['policyDetails']
    return current_policy_details['name']
