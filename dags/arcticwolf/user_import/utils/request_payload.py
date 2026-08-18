import uuid
from datetime import datetime
import rail


null = None


def get_first_name(dag_run):
    if dag_run.conf['firstname'] and ((dag_run.conf['firstname']).lower() != ((rail.result('bulk_get_users3')[
            0]['userDetails']['firstName']).lower() if rail.result('bulk_get_users3')[0]['userDetails']['firstName'] else '')):
        return {"value": dag_run.conf['firstname']}
    return null


def get_last_name(dag_run):
    if dag_run.conf['lastname'] and ((dag_run.conf['lastname']).lower() != ((rail.result('bulk_get_users3')[
            0]['userDetails']['lastName']).lower() if rail.result('bulk_get_users3')[0]['userDetails']['lastName'] else '')):
        return {"value": dag_run.conf['lastname']}
    return null


def get_email_address(dag_run):
    if dag_run.conf['emailaddress'] and ((dag_run.conf['emailaddress']).lower() != ((rail.result('bulk_get_users3')[0]['userDetails']['emailAddress']).lower()
                                                                                    if rail.result('bulk_get_users3')[0]['userDetails']['emailAddress'] else '')):
        return {"value": dag_run.conf['emailaddress']}
    return null


def get_employment_daterange(dag_run):
    payload = {
        "value": {
            "startDate": null,
            "endDate": null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }

    rundate = {
        "year": dag_run.conf['rundate']['year'],
        "month": dag_run.conf['rundate']['month'],
        "day": dag_run.conf['rundate']['day']
    }

    if dag_run.conf['status'] == 'Terminated':
        payload['value']['endDate'] = dag_run.conf['enddate'] if dag_run.conf['enddate'] else rundate
        payload['value']['startDate'] = rail.result('bulk_get_users3')[0]['userDetails']['employmentDateRange']['startDate'] if rail.result('bulk_get_users3')[
            0]['userDetails']['employmentDateRange']['startDate'] else null

    if not (rail.result('bulk_get_users3')[0]['userDetails']['employmentDateRange']['startDate'] and rail.result(
        'bulk_get_users3')[0]['userDetails']['employmentDateRange']['startDate']['day']) or (datetime.strptime((str(rail.result('bulk_get_users3')[
            0]['userDetails']['employmentDateRange']['startDate']['year']) + '-' + str(rail.result(
                'bulk_get_users3')[0]['userDetails']['employmentDateRange']['startDate']['month']) + '-' + str(rail.result(
                    'bulk_get_users3')[0]['userDetails']['employmentDateRange']['startDate']['day'])), '%Y-%m-%d') != datetime.strptime(dag_run.conf['userstartdate'], '%Y-%m-%d')):

        payload['value']['startDate'] = dag_run.conf['startdate']

    if (rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'] and rail.find_first_by_attr_and_get_attr(
            rail.result('bulk_get_users3')[
                0]['userDetails']['customFieldValues'],
            'customField.name', 'Status', 'text', '') == 'Terminated' and dag_run.conf['status'] == 'Active'):
        payload['value']['endDate'] = null

    if payload['value']['startDate'] or payload['value']['endDate']:
        return payload

    return null


def get_security_settings(dag_run):
    if dag_run.conf['status'] == 'On Leave':
        return {
            "value": {
                "loginEnabled": {
                    "value": "false"
                },
                "forcePasswordChange": null,
                "ssoName": null,
                "ssoNameModificationOptionUri": null,
                "password": null,
                "authenticationProviders": [],
                "emailMFAResendVerificationEmail": null,
                "emailMFATryAddMethodFromUsersEmail": null,
                "isMFAMethodRequired": null,
                "clearIsLockedOut": null
            }
        }
    if (rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'] and rail.find_first_by_attr_and_get_attr(
            rail.result('bulk_get_users3')[
                0]['userDetails']['customFieldValues'],
        'customField.name', 'Status', 'text', '') == 'On Leave' and dag_run.conf['status'] == 'Active') or (rail.result('bulk_get_users3')[
            0]['userDetails']['customFieldValues'] and rail.find_first_by_attr_and_get_attr(
            rail.result('bulk_get_users3')[
                0]['userDetails']['customFieldValues'],
            'customField.name', 'Status', 'text', '') == 'Terminated' and dag_run.conf['status'] == 'Active'):
        return {
            "value": {
                "loginEnabled": {
                    "value": "true"
                },
                "forcePasswordChange": null,
                "ssoName": null,
                "ssoNameModificationOptionUri": null,
                "password": null,
                "authenticationProviders": [],
                "emailMFAResendVerificationEmail": null,
                "emailMFATryAddMethodFromUsersEmail": null,
                "isMFAMethodRequired": null,
                "clearIsLockedOut": null
            }
        }
    return null


def get_employeetype_group(dag_run):
    if dag_run.conf['employeetype'] and (not (rail.result('get_effective_user_group_membership') and rail.result(
        'get_effective_user_group_membership')['employeeTypes'] and rail.result(
        'get_effective_user_group_membership')['employeeTypes'][0]['employeeType']) or (
        rail.result(
            'get_effective_user_group_membership')['employeeTypes'][0]['employeeType']['employeeType']['displayText'] != dag_run.conf['employeetype'])):
        return [
            {
                "dateRange": {
                    "startDate": {
                        "year": dag_run.conf['rundate']['year'],
                        "month": dag_run.conf['rundate']['month'],
                        "day": dag_run.conf['rundate']['day']
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "item": {
                    "uri": null,
                    "parent": null,
                    "name": dag_run.conf['employeetype'],
                    "parameterCorrelationId": null
                }
            }
        ]

    return []


def get_cost_center_group(dag_run):
    if dag_run.conf['cost_center'] and (not (rail.result('get_effective_user_group_membership') and rail.result(
        'get_effective_user_group_membership')['costCenters'] and rail.result(
        'get_effective_user_group_membership')['costCenters'][0]['costCenter']) or (
        rail.result(
            'get_effective_user_group_membership')['costCenters'][0]['costCenter']['costCenter']['displayText'] != dag_run.conf['cost_center'])):
        return [{
                "dateRange": {
                    "startDate": {
                        "year": dag_run.conf['rundate']['year'],
                        "month": dag_run.conf['rundate']['month'],
                        "day": dag_run.conf['rundate']['day']
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "item": {
                    "uri": null,
                    "parentUri": null,
                    "name": dag_run.conf['cost_center']
                }
                }]

    return []


def get_service_center_group(dag_run):
    if dag_run.conf['position_title'] and (not (rail.result('get_effective_user_group_membership') and rail.result(
        'get_effective_user_group_membership')['serviceCenters'] and rail.result(
        'get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter']) or (
        rail.result(
            'get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter']['serviceCenter']['displayText'] != dag_run.conf['position_title'])):
        return [{
                "dateRange": {
                    "startDate": {
                        "year": dag_run.conf['rundate']['year'],
                        "month": dag_run.conf['rundate']['month'],
                        "day": dag_run.conf['rundate']['day']
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "item": {
                    "uri": null,
                    "parentUri": null,
                    "name": dag_run.conf['position_title']
                }
                }]

    return []


def get_division_group_schedule(dag_run):
    if dag_run.conf['division'] and (not (rail.result('get_effective_user_group_membership') and rail.result(
        'get_effective_user_group_membership')['divisions'] and rail.result(
        'get_effective_user_group_membership')['divisions'][0]['division']) or (
        rail.result(
            'get_effective_user_group_membership')['divisions'][0]['division']['division']['displayText'] != dag_run.conf['division'])):
        return [{
                "dateRange": {
                    "startDate": {
                        "year": dag_run.conf['rundate']['year'],
                        "month": dag_run.conf['rundate']['month'],
                        "day": dag_run.conf['rundate']['day']
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "item": {
                    "uri": null,
                    "parentUri": null,
                    "name": dag_run.conf['division']
                }
                }]

    return []


def get_department_group_schedule(dag_run):
    if dag_run.conf['departmentgroupuri'] and (not (rail.result('get_effective_user_group_membership') and rail.result(
        'get_effective_user_group_membership')['departments'] and rail.result(
        'get_effective_user_group_membership')['departments'][0]['department']) or (
        rail.result(
            'get_effective_user_group_membership')['departments'][0]['department']['department']['uri'] != dag_run.conf['departmentgroupuri'])):
        return [{
                "dateRange": {
                    "startDate": {
                        "year": dag_run.conf['rundate']['year'],
                        "month": dag_run.conf['rundate']['month'],
                        "day": dag_run.conf['rundate']['day']
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "item": {
                    "uri": dag_run.conf['departmentgroupuri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                }
                }]

    return []


def get_location_schedule(dag_run):
    if dag_run.conf['locationuri'] and (not (rail.result('get_effective_user_group_membership') and rail.result(
        'get_effective_user_group_membership')['locations'] and rail.result(
        'get_effective_user_group_membership')['locations'][0]['location']) or (
        rail.result(
            'get_effective_user_group_membership')['locations'][0]['location']['location']['uri'] != dag_run.conf['locationuri'])):
        return [{
                "dateRange": {
                    "startDate": {
                        "year": dag_run.conf['rundate']['year'],
                        "month": dag_run.conf['rundate']['month'],
                        "day": dag_run.conf['rundate']['day']
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "item": {
                    "uri": dag_run.conf['locationuri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                }
                }]

    return []


def get_holiday_calendar(dag_run, config):
    holiday_calendar_name = next((item['value'] for item in config.USER_IMPORT_MAPPER if item['type']
                                 == 'holiday_cal' and item['code'] == dag_run.conf['location_level_1']), None)
    if holiday_calendar_name and ((holiday_calendar_name).lower() != ((rail.result('bulk_get_users3')[
            0]['holidayCalendar']['name']).lower() if rail.result('bulk_get_users3')[0]['holidayCalendar'] and rail.result('bulk_get_users3')[0]['holidayCalendar']['name'] else '')):
        return {
            "value": {
                "uri": null,
                "name": holiday_calendar_name
            }
        }

    return null


def get_payrule_schedule(dag_run, config):
    payrule_name = next((item['value'] for item in config.USER_IMPORT_MAPPER if item['type']
                        == 'pay_rule' and item['code'] == dag_run.conf['position_title_code']), None)
    if payrule_name and ((payrule_name).lower() != ((rail.result('bulk_get_users3')[
            0]['payRuleScriptSchedule'][0]['payRuleScript']['displayText']).lower() if rail.result('bulk_get_users3')[0]['payRuleScriptSchedule'] and rail.result('bulk_get_users3')[0]['payRuleScriptSchedule'][0]['payRuleScript']['displayText'] else '')):
        return [
            {
                "dateRange": null,
                "item": {
                    "uri": null,
                    "name": payrule_name
                }
            }
        ]
    return []


def get_schedule_type_schedule(dag_run):
    if dag_run.conf['position_title'] and dag_run.conf['position_title_code'] and dag_run.conf['position_title_code'][-2] in ['M', 'E'] and (not (
        rail.result('get_effective_user_group_membership') and rail.result(
            'get_effective_user_group_membership')['serviceCenters'] and rail.result(
            'get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter']) or (
        rail.result(
            'get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter']['serviceCenter']['displayText'] != dag_run.conf['position_title'])):
        return [
            {
                "dateRange": null,
                "item": {
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                    "officeSchedule": {
                        "officeScheduleUri": null,
                        "name": "8 hours/day; Mon-Fri"
                    }
                }
            }
        ]
    return []


def get_policy_sets(dag_run):
    if dag_run.conf['position_title'] and dag_run.conf['position_title_code'] and dag_run.conf['position_title_code'][-2] in ['M', 'E'] and (not (
        rail.result('get_effective_user_group_membership') and rail.result(
            'get_effective_user_group_membership')['serviceCenters'] and rail.result(
            'get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter']) or (
        rail.result(
            'get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter']['serviceCenter']['displayText'] != dag_run.conf['position_title'])):
        return [
            {
                "modificationOptionUri": "urn:replicon:collection-modification-option:remove",
                "items": [
                    {
                        "uri": null,
                        "name": "Standard Timesheet"
                    }
                ]
            }
        ]
    return []


def get_custom_fields(dag_run):
    custom_fields_list = []

    custom_fields_key_dict = {
        "Job Code": "jobcode",
        "FTE": "fte",
        "Exemption Status": "exemption_status",
        "Status": "status",
        "Worker Type": "type_worker"
    }

    for field_name, value in custom_fields_key_dict.items():
        if dag_run.conf[value] and rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'] and rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')[
                    0]['userDetails']['customFieldValues'],
                'customField.name', field_name, 'text', '') != dag_run.conf[value]:
            custom_fields_list.append({
                "value": {
                    "customField": {
                        "uri": null,
                        "name": field_name
                    },
                    "text": dag_run.conf[value],
                    "date": null,
                    "dropDownOption": null,
                    "number": null
                }
            })

    if rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'] and rail.find_first_by_attr_and_get_attr(
        rail.result('bulk_get_users3')[
            0]['userDetails']['customFieldValues'],
            'customField.name', 'Last Hire Date', 'date', '') != dag_run.conf['last_hire_date']:
        custom_fields_list.append({
            "value": {
                "customField": {
                    "uri": null,
                    "name": 'Last Hire Date'
                },
                "text": null,
                "date": dag_run.conf['last_hire_date'],
                "dropDownOption": null,
                "number": null
            }
        })
    return custom_fields_list


def get_timezone(dag_run, config):
    timezoneuri = next((item['uri'] for item in config.USER_IMPORT_MAPPER if item['type']
                       == 'time_zone' and item['code'] == dag_run.conf['location_level_1']), None)
    if timezoneuri and rail.result('bulk_get_users3')[0]['timeZone']['uri'] != timezoneuri:
        return {
            "value": {
                "uri": timezoneuri,
                "IANAName": null
            }
        }
    return null


def get_timesheet_period_schedule(dag_run):
    if (rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'] and rail.find_first_by_attr_and_get_attr(
            rail.result('bulk_get_users3')[
                0]['userDetails']['customFieldValues'],
            'customField.name', 'Status', 'text', '') == 'Terminated' and dag_run.conf['status'] == 'Active'):
        return [
            {
                "dateRange": {
                    "startDate": dag_run.conf['startdate'],
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "item": {
                    "uri": null,
                    "name": "Weekly starting on Monday"
                }
            }
        ]
    return []


def user_update_payload_schema(dag_run, config):

    payload = {
        "target": {
            "uri": null,
            "loginName": null,
            "employeeId": dag_run.conf['employeeid'],
            "parameterCorrelationId": null
        },
        "template": null,
        "modifications": {
            "firstName": get_first_name(dag_run),
            "lastName": get_last_name(dag_run),
            "loginName": null,
            "displayName": null,
            "emailAddress": get_email_address(dag_run),
            "employeeId": null,
            "employmentDateRange": get_employment_daterange(dag_run),
            "securitySettings": get_security_settings(dag_run),
            "timesheetApprovalPath": null,
            "timeEntryApprovalPath": null,
            "workAuthorizationApprovalPath": null,
            "timeoffApprovalPath": null,
            "timeOffBalancePayoutApprovalPath": null,
            "defaultActivity": null,
            "expenseApprovalPath": null,
            "timeZone": get_timezone(dag_run, config),
            "workWeekStartDay": null,
            "defaultBillingRate": null,
            "userPreferences": null,
            "formattings": null,
            "notificationPreferences": null,
            "timesheetTemplate": null,
            "timeoffTemplate": null,
            "timeOffCalendarVisibility": null,
            "expenseTemplate": null,
            "workAuthorizationTemplate": null,
            "punchEntryPolicy": null,
            "holidayCalendar": get_holiday_calendar(dag_run, config),
            "extensionFields": [],
            "customFields": get_custom_fields(dag_run),
            "products": [],
            "skills": [],
            "activities": [],
            "policySets": get_policy_sets(dag_run),
            "permissionSets": [],
            "bankedTimePolicies": [],
            "timeOffTypes": [],
            "locationSchedule": get_location_schedule(dag_run),
            "divisionSchedule": get_division_group_schedule(dag_run),
            "costCenterSchedule": get_cost_center_group(dag_run),
            "serviceCenterSchedule": get_service_center_group(dag_run),
            "departmentGroupSchedule": get_department_group_schedule(dag_run),
            "employeeTypeGroupSchedule": get_employeetype_group(dag_run),
            "supervisorSchedule": [],
            "timesheetPeriodSchedule": [],
            "holidayCalendarSchedule": [],
            "scheduleTypeSchedule": get_schedule_type_schedule(dag_run),
            "payRuleSchedule":  get_payrule_schedule(dag_run, config),
            "placeSchedule": [],
            "payRateSchedule": [],
            "projectRoleSchedule": [],
            "costNormalizationRuleSchedule": [],
            "hourlyRatesSchedule": [],
            "substituteUserSchedule": []
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

    return payload


def get_timezone_for_add_user(dag_run, config):
    timezoneuri = next((item['uri'] for item in config.USER_IMPORT_MAPPER if item['type']
                       == 'time_zone' and item['code'] == dag_run.conf['location_level_1']), None)
    if timezoneuri:
        return {
            "value": {
                "uri": timezoneuri,
                "IANAName": null
            }
        }
    return null


def get_holiday_calendar_for_add_user(dag_run, config):
    holiday_calendar_name = next((item['value'] for item in config.USER_IMPORT_MAPPER if item['type']
                                 == 'holiday_cal' and item['code'] == dag_run.conf['location_level_1']), None)
    if holiday_calendar_name:
        return {
            "value": {
                "uri": null,
                "name": holiday_calendar_name
            }
        }

    return null


def get_schedule_type_schedule_add_user(dag_run):
    if dag_run.conf['position_title_code'] and (dag_run.conf['position_title_code'] in ['188', 'C188'] or dag_run.conf['position_title_code'][-2] not in ['M', 'E']):
        return [
            {
                "dateRange": null,
                "item": {
                    "scheduleTypeUri": "urn:replicon:schedule-type:shift",
                    "officeSchedule": null
                }
            }
        ]
    if dag_run.conf['position_title_code'] and dag_run.conf['position_title_code'][-2] in ['M', 'E']:
        return [
            {
                "dateRange": null,
                "item": {
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                    "officeSchedule": {
                        "officeScheduleUri": null,
                        "name": "8 hours/day; Mon-Fri"
                    }
                }
            }
        ]
    return []


def get_all_enabled_activities_for_assignment():
    return [
        {
            "modificationOptionUri": "urn:replicon:collection-modification-option:add",
            "items": [
                {
                    "uri": null,
                    "name": activity['name'],
                    "code": null
                } for activity in rail.result('get_all_enabled_activities')
            ]
        }
    ]


def get_custom_fields_for_add_user(dag_run):
    custom_fields_list = []

    custom_fields_key_dict = {
        "Job Code": "jobcode",
        "FTE": "fte",
        "Exemption Status": "exemption_status",
        "Status": "status",
        "Worker Type": "type_worker"
    }

    for field_name, value in custom_fields_key_dict.items():
        if dag_run.conf[value]:
            custom_fields_list.append({
                "value": {
                    "customField": {
                        "uri": null,
                        "name": field_name
                    },
                    "text": dag_run.conf[value],
                    "date": null,
                    "dropDownOption": null,
                    "number": null
                }
            })

    if dag_run.conf['last_hire_date']:
        custom_fields_list.append({
            "value": {
                "customField": {
                    "uri": null,
                    "name": 'Last Hire Date'
                },
                "text": null,
                "date": dag_run.conf['last_hire_date'],
                "dropDownOption": null,
                "number": null
            }
        })
    return custom_fields_list


def user_add_payload_schema(dag_run, config):
    payload = {
        "target": null,
        "template": null,
        "modifications": {
            "firstName": {
                "value": dag_run.conf['firstname']
            },
            "lastName": {
                "value": dag_run.conf['lastname']
            },
            "loginName": {
                "value": dag_run.conf['emailaddress']
            },
            "displayName": null,
            "emailAddress": {
                "value": dag_run.conf['emailaddress']
            },
            "employeeId": {
                "value": dag_run.conf['employeeid']
            },
            "employmentDateRange": {
                "value": {
                    "startDate": dag_run.conf['startdate'],
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": "true"
                    },
                    "forcePasswordChange": null,
                    "ssoName": {
                        "value": dag_run.conf['emailaddress']
                    },
                    "ssoNameModificationOptionUri": null,
                    "password": null,
                    "authenticationProviders": [],
                    "emailMFAResendVerificationEmail": null,
                    "emailMFATryAddMethodFromUsersEmail": null,
                    "isMFAMethodRequired": null,
                    "clearIsLockedOut": null
                },
            },
            "timesheetApprovalPath": null,
            "timeEntryApprovalPath": null,
            "workAuthorizationApprovalPath": null,
            "timeoffApprovalPath": {
                "value": {
                    "uri": null,
                    "name": "Auto Approved"
                }
            },
            "timeOffBalancePayoutApprovalPath": null,
            "defaultActivity": null,
            "expenseApprovalPath": null,
            "timeZone": get_timezone_for_add_user(dag_run, config),
            "workWeekStartDay": null,
            "defaultBillingRate": null,
            "userPreferences": null,
            "formattings": null,
            "notificationPreferences": null,
            "timesheetTemplate": {
                "value": {
                    "uri": null,
                    "name": "Standard Timesheet"
                }
            } if (dag_run.conf['position_title_code'] not in ['188', 'C188'] and
                  dag_run.conf['position_title_code'][-2] not in ['E', 'M']) else null,
            "timeoffTemplate": {
                "value": {
                    "uri": null,
                    "name": "Time Off"
                }
            },
            "timeOffCalendarVisibility": null,
            "expenseTemplate": null,
            "workAuthorizationTemplate": null,
            "punchEntryPolicy": null,
            "holidayCalendar": get_holiday_calendar_for_add_user(dag_run, config),
            "extensionFields": [],
            "customFields": get_custom_fields_for_add_user(dag_run),
            "products": [],
            "skills": [],
            "activities": get_all_enabled_activities_for_assignment(),
            "policySets": [],
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "uri": null,
                                "name": "ZT User"
                            },
                            "groupAccessFilter": null
                        }
                    ]
                },
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "uri": null,
                                "name": "Project Resource"
                            },
                            "groupAccessFilter": null
                        }
                    ]
                }
            ],
            "bankedTimePolicies": [],
            "timeOffTypes": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "timeOffType": {
                                "uri": null,
                                "name": "State Holiday"
                            },
                            "isTimeOffAllowedAgainstThisTimeOffType": "true",
                            "applyDefaultTimeOffTypePolicy": "true",
                            "defaultTimeOffTypePolicyEffectiveDate": null,
                            "policySchedule": []
                        }
                    ]
                },
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "timeOffType": {
                                "uri": null,
                                "name": "PTO/Vacation"
                            },
                            "isTimeOffAllowedAgainstThisTimeOffType": "true",
                            "applyDefaultTimeOffTypePolicy": "true",
                            "defaultTimeOffTypePolicyEffectiveDate": null,
                            "policySchedule": []
                        }
                    ]
                },
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "timeOffType": {
                                "uri": null,
                                "name": "Other"
                            },
                            "isTimeOffAllowedAgainstThisTimeOffType": "true",
                            "applyDefaultTimeOffTypePolicy": "true",
                            "defaultTimeOffTypePolicyEffectiveDate": null,
                            "policySchedule": []
                        }
                    ]
                },
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "timeOffType": {
                                "uri": null,
                                "name": "Volunteer"
                            },
                            "isTimeOffAllowedAgainstThisTimeOffType": "true",
                            "applyDefaultTimeOffTypePolicy": "true",
                            "defaultTimeOffTypePolicyEffectiveDate": null,
                            "policySchedule": []
                        }
                    ]
                }
            ],
            "locationSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": dag_run.conf['locationuri'],
                        "parentUri": null,
                        "name": null
                    }
                }
            ] if dag_run.conf['locationuri'] else [],
            "divisionSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf['division']
                    }
                }
            ] if dag_run.conf['division'] else [],
            "costCenterSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf['cost_center']
                    }
                }
            ] if dag_run.conf['cost_center'] else [],
            "serviceCenterSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf['position_title']
                    }
                }
            ] if dag_run.conf['position_title'] else [],
            "departmentGroupSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": dag_run.conf['departmentgroupuri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    }
                }
            ] if dag_run.conf['departmentgroupuri'] else [],
            "employeeTypeGroupSchedule":  [
                {
                    "dateRange": null,
                    "item": {
                        "uri": null,
                        "parent": null,
                        "name": dag_run.conf['employeetype'],
                        "parameterCorrelationId": null
                    }
                }
            ],
            "supervisorSchedule": [],
            "timesheetPeriodSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": null,
                        "name": "Weekly starting on Monday"
                    }
                }
            ],
            "holidayCalendarSchedule": [],
            "scheduleTypeSchedule": get_schedule_type_schedule_add_user(dag_run),
            "payRuleSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": null,
                        "name": next((item['value'] for item in config.USER_IMPORT_MAPPER if item['type'] == 'pay_rule' and item['code'] == dag_run.conf['position_title_code']), None)
                    }
                }
            ] if next((item['value'] for item in config.USER_IMPORT_MAPPER if item['type'] == 'pay_rule' and item['code'] == dag_run.conf['position_title_code']), None) else [],
            "placeSchedule": [],
            "payRateSchedule": [],
            "projectRoleSchedule": [],
            "costNormalizationRuleSchedule": [],
            "hourlyRatesSchedule": [],
            "substituteUserSchedule": []
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }
    return payload


def get_search_supervisor_payload(dag_run):
    return {
        "page": "1",
                "pagesize": "1000",
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
                            "text": dag_run.conf['supervisor'],
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
