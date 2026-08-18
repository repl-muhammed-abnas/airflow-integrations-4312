from datetime import datetime, timedelta
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods \
    import convert_json_date_to_date, get_ia_update_payload_for_udf_update, compare_if_two_json_dates_are_same as _compare_if_two_json_dates_are_same
from dxctechnology.workday_user_import_v1.user_import.common_utils.request_payload import get_todays_date_in_json, get_psa_udf_value

import rail

null = None
INPUT_DATE_FORMAT = "%Y-%d-%m"

# Default fallback value for IA=1 users when mapper value is not available
NONE_DEFAULT_VALUE = "NONE"


def _is_international_assignee(is_ia):
    return is_ia in [1, '1']


# UDF fields that are NOT applicable to Portugal region (same as Global)
PORTUGAL_EXCLUDED_UDFS = (
    "Work Shift",
    "Date of Birth",
    "Time Type",
    "Middle Name",
    "Annual Leave Anni. Date",
    "LSL Anniversary Date",
    "Personal Leave Anni. Date",
    "Weekly Scheduled Hours",
    "Employee Group",
    "Employee Sub Group",
    "Terms and Conditions",
    "Termination Reason",
    "Termination Reason Code",
    "RUT",
    "EE Group",
)


def _get_udf_fields_to_clear_for_ia(dag_run, current_custom_fields_values, exception_log):
    fields_to_clear = []
    is_ia = dag_run.conf['file_data'].get('is_ia')

    if not _is_international_assignee(is_ia):
        return fields_to_clear

    for udf_display_text in PORTUGAL_EXCLUDED_UDFS:
        for cf in current_custom_fields_values:
            if cf.get('customField', {}).get('displayText') == udf_display_text:
                has_value = cf.get('text') or cf.get('date')
                if has_value:
                    exception_log.append(f"Clearing UDF '{udf_display_text}' for IA=1 user (not applicable to Portugal region)")
                    fields_to_clear.append({
                        "customField": {
                            "uri": cf['customField']['uri'],
                            "displayText": null,
                            "parameterCorrelationId": null
                        },
                        "text": "",
                        "date": null,
                        "dropDownOptionValue": null
                    })
                break

    return fields_to_clear


def get_replicon_date(date_str, return_format= "dict", _date_format= INPUT_DATE_FORMAT):

    _date = datetime.strptime(date_str, _date_format)

    if return_format == "date":
        return _date

    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }

def _get_email_to_add(dag_run, config):
    if config.instance in ["prod", "production", "trial"]:
        return dag_run.conf['file_data']['email_id']
    return null

def _get_shift_from_mapper(dag_run, config):
    country = dag_run.conf['file_data']['country']
    source = dag_run.conf['file_data']['parent_company']
    return list(filter(lambda row: row['Type'] == "Schedule Type" and\
                                row['Function'] == "Workday User Sync" and\
                                row['Value'] == "Shift" and\
                                row['Country'] == country and\
                                row['Source'] == source,config.MAPPER))

def _get_schedule_policy_to_assign(dag_run, config, exception_log:list):
    if dag_run.conf['schedule_data']['schedule_name'] == "Shift Schedule":
        shift_data = _get_shift_from_mapper(dag_run, config)
        return [
            {
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": null,
                    # Workato this will not fail until the service call is made
                    # to have the same, added below condition
                    "scheduleTypeUri": shift_data[0]['URI'] if shift_data else null
                },
                "effectiveDate": null
            }
        ]
    else:
        if dag_run.conf['schedule_data']['schedule_name']:
            if dag_run.conf['schedule_data']['schedule_uri']:
                return [
                    {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": dag_run.conf['schedule_data']['schedule_name'],
                                "officeSchedule": {
                                    "officeScheduleUri": null,
                                    "name": dag_run.conf['schedule_data']['schedule_name']
                                },
                                "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri']
                            },
                            "effectiveDate": null
                        }
                ]
            else:
                exception_log.append(f"""Office schedule "{dag_run.conf['schedule_data']['schedule_name']}" not available in Replicon. Hence default shift assigned""")
                return [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf['schedule_data']['office_schedule'],
                            "officeSchedule": {
                                "officeScheduleUri": null,
                                "name": dag_run.conf['schedule_data']['office_schedule']
                            },
                            "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri']
                        },
                        "effectiveDate": null
                    }
                ]
        else:
            if dag_run.conf['schedule_data']['office_schedule']:
                return [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf['schedule_data']['office_schedule'],
                            "officeSchedule": {
                                "officeScheduleUri": null,
                                "name": dag_run.conf['schedule_data']['office_schedule']
                            },
                            "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri']
                        },
                        "effectiveDate": null
                    }
                ]
    return [
        {
            "schedulePolicy": {
                "officeScheduleUri": null,
                "name": "7.5 hours/day, Fri, Sa off",
                "officeSchedule": {
                    "officeScheduleUri": null,
                    "name": "7.5 hours/day, Fri, Sa off"
                },
                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
            },
            "effectiveDate": null
        }
    ]

def _get_user_permission_to_assign(dag_run):
    if dag_run.conf['user_permission_sets']['end_user_permission']:
        return [
            {
                "uri": dag_run.conf['user_permission_sets']['end_user_permission']['uri'],
                "name": null
            }
        ]
    return []

def _get_policy_sets_to_assign(dag_run):
    policy_sets = []
    if dag_run.conf['policy_sets']['timeoff_template'] and dag_run.conf['mapper_data']['profile_status'].lower() == "enabled":
        policy_sets.append({
            "name": dag_run.conf['policy_sets']['timeoff_template']['name'],
            "uri": null
        })

    if dag_run.conf['mapper_data']['timesheet_template'] and dag_run.conf['mapper_data']['profile_status'].lower() == "enabled":
        if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
            policy_sets.append({
                "name": dag_run.conf['mapper_data']['timesheet_template'],
                "uri": null
            })
    
    if dag_run.conf['policy_sets']['punch_entry_policy'].get('uri'):
        policy_sets.append({
            "name": null,
            "uri": dag_run.conf['policy_sets']['punch_entry_policy']['uri']
        })

    return policy_sets

def _get_timesheet_approval_path(dag_run):
    if dag_run.conf['mapper_data']['timesheet_approval_path']:
        return {
            "uri": null,
            "name": dag_run.conf['mapper_data']['timesheet_approval_path']
        }

    return null

def _get_timeoff_approval_to_assign(dag_run):
    if dag_run.conf['mapper_data']['timeoff_approval']:
        return {
            "uri": null,
            "name": dag_run.conf['mapper_data']['timeoff_approval']
        }

    return null

def _get_work_week_to_assign(dag_run):
    if dag_run.conf['mapper_data']['work_week_uri']:
        return dag_run.conf['mapper_data']['work_week_uri']
    return null 

def _get_holiday_calendar_to_assign(dag_run):
    if dag_run.conf['mapper_data']['holiday_calendar_uri']:
        return {
            "uri": dag_run.conf['mapper_data']['holiday_calendar_uri']['uri'],
            "name": null
        }
    return null

def _get_employee_type_uri_to_assign(dag_run):
    if dag_run.conf['groups']['employee_type'].get('uri'):
        return [
            {
                "employeeTypeGroup": {
                    "uri": dag_run.conf['groups']['employee_type']['uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }
        ]
    return null

def _get_payrule_to_assign(dag_run):
    if dag_run.conf['payrule']['payrule']:
        if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
            return [
                {
                    "payRuleScript": {
                    "uri": null,
                        "name": dag_run.conf['payrule']['payrule']
                    },
                    "effectiveDate": null
                }
            ]
    return null

def _get_cost_center_to_apply(dag_run):
    if dag_run.conf['groups']['cost_center'].get('uri'):
        return [
            {
                "costCenter": {
                    "uri": dag_run.conf['groups']['cost_center']['uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }
        ]
    return null

def _get_department_to_assign(dag_run):
    if dag_run.conf['groups']['department'].get('uri'):
        return [
            {
                "departmentGroup": {
                    "uri": dag_run.conf['groups']['department']['uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }
        ]

    return null

def _get_location_to_assign(dag_run):
    if dag_run.conf['groups']['location'].get("uri"):
        return [
            {
                "location": {
                    "uri": dag_run.conf['groups']['location']['uri'],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }
        ]
    return null

def _get_division_to_assign(dag_run):
    if dag_run.conf['groups']['division'].get('uri'):
        return [
            {
                "division": {
                    "uri":  dag_run.conf['groups']['division']['uri'],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }
        ]
    
    return null

def _get_service_center_to_assign(dag_run):
    if dag_run.conf['file_data']['pay_group']:
        return [
            {
                "serviceCenter": {
                    "uri": null,
                    "parentUri": null,
                    "name": dag_run.conf['file_data']['pay_group']
                },
                "effectiveDate": null
            }
        ]
    return null

def _get_activity_list_to_assign(dag_run):
    activity_list = dag_run.conf['activities']['activity'].split("|")
    
    return list(map(lambda activity: {
        "uri": null,
        "name": activity
    }, activity_list))

def _add_custom_field(custom_field_uri, text=null, date=null, drop_down_uri=null,drop_down_name=null, number=null):
    return {
        "customField": {
            "uri" : custom_field_uri
        },
        "text": text,
        "date": date,
        "dropDownOption": {
            "uri": drop_down_uri,
            "name": drop_down_name
        } if drop_down_uri or drop_down_name else null,
        "number": number   
    }

def _get_custom_fields_to_assign(dag_run):
    custom_fields = dag_run.conf['udfs']
    
    udfs_to_assign = [_add_custom_field(custom_fields['perner']['uri'], text=dag_run.conf['file_data']['emp_id'])]
    
    if dag_run.conf['file_data']['assignment_type']:
        udfs_to_assign.append(_add_custom_field(custom_fields['assignment_type']['uri'], text=dag_run.conf['file_data']['assignment_type']))
    
    if dag_run.conf['file_data']['perner_id']:
        udfs_to_assign.append(_add_custom_field(custom_fields['ia_perner_id']['uri'], text=dag_run.conf['file_data']['perner_id']))
        
    work_shift:str = dag_run.conf['file_data']['work_shift']
    if work_shift:
        drop_down_value = "BPSOT" if work_shift.startswith("BPSOT") else ("BPS" if work_shift.startswith("BPS") else work_shift)
        udfs_to_assign.append(_add_custom_field(custom_fields['employee_type_udf']['uri'], drop_down_name=drop_down_value))
        udfs_to_assign.append(_add_custom_field(custom_fields['work_shift']['uri'], text=dag_run.conf['file_data']['work_shift']))
    
    if dag_run.conf['file_data']['dob']:
        udfs_to_assign.append(_add_custom_field(custom_fields['date_of_birth']['uri'], date=dag_run.conf['json_formatted_dates']['date_of_birth']))
    
    if dag_run.conf['file_data']['rut']:
        udfs_to_assign.append(_add_custom_field(custom_fields['rut']['uri'], text=dag_run.conf['file_data']['rut']))

    if dag_run.conf['file_data']['time_type']:
        udfs_to_assign.append(_add_custom_field(custom_fields['time_type']['uri'], drop_down_name=dag_run.conf['file_data']['time_type']))

    if dag_run.conf['file_data']['gender']:
        udfs_to_assign.append(_add_custom_field(custom_fields['gender']['uri'], text=dag_run.conf['file_data']['gender']))
    
    if dag_run.conf['file_data']['on_leave']:
        udfs_to_assign.append(_add_custom_field(custom_fields['on_leave']['uri'], text=dag_run.conf['file_data']['on_leave']))
        
    if dag_run.conf['file_data']['area_code']:
        udfs_to_assign.append(_add_custom_field(custom_fields['personnel_area_code']['uri'], text=dag_run.conf['file_data']['area_code']))
    
    if dag_run.conf['file_data']['area_name']:
        udfs_to_assign.append(_add_custom_field(custom_fields['personnel_area_name']['uri'], text=dag_run.conf['file_data']['area_name']))
    
    if dag_run.conf['file_data']['job_level']:
        job_level_prefix = "H" if dag_run.conf['file_data']['country']=="Canada" else ""
        udfs_to_assign.append(_add_custom_field(custom_fields['job_level']['uri'], text=f"{job_level_prefix}{dag_run.conf['file_data']['job_level']}"))
    
    if dag_run.conf['file_data']['fte']:
        udfs_to_assign.append(_add_custom_field(custom_fields['fte']['uri'], text=dag_run.conf['file_data']['fte']))
        
    if dag_run.conf['file_data']['management_lvl']:
        udfs_to_assign.append(_add_custom_field(custom_fields['management_level']['uri'], text=dag_run.conf['file_data']['management_lvl']))
        
    if dag_run.conf['file_data']['fte_pct']:
        udfs_to_assign.append(_add_custom_field(custom_fields['ftepct']['uri'], text=dag_run.conf['file_data']['fte_pct']))
        
    if dag_run.conf['file_data']['is_ia']:
        udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee']['uri'], text=dag_run.conf['file_data']['is_ia']))
    
    if dag_run.conf['file_data']['service_date']:
        udfs_to_assign.append(_add_custom_field(custom_fields['service_date']['uri'], date=get_replicon_date(dag_run.conf['file_data']['service_date'])))
    
    if dag_run.conf['file_data']['ia_start_date']:
        udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_start_date']['uri'], date=get_replicon_date(dag_run.conf['file_data']['ia_start_date'])))
    
    if not dag_run.conf['file_data']['ia_start_date'] and dag_run.conf['file_data']['is_ia'] in [1, '1']:
        udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_start_date']['uri'], date=get_todays_date_in_json()))
    
    if dag_run.conf['file_data']['ia_end_date']:
        udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_end_date']['uri'], date=get_replicon_date(dag_run.conf['file_data']['ia_end_date'])))
    
    return udfs_to_assign

def _get_is_login_enabled(dag_run):
    if (not dag_run.conf['allowed_country']) or (dag_run.conf['allowed_country'].lower() != "enable") or\
        (not dag_run.conf['file_data']['parent_company']) or (not dag_run.conf['mapper_data']['profile_status']) or\
            (dag_run.conf['mapper_data']['profile_status'] != "enabled"):
        return False
    return dag_run.conf['replicon_field'] == "true"

def _get_timesheet_period_schedule_to_assign(dag_run):
    if dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']:
        if convert_json_date_to_date(dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']) >\
            convert_json_date_to_date(dag_run.conf['json_formatted_dates']['hire_date']):
                return [
                    {
                        "timesheetPeriod": {
                            "uri": null,
                            "name": dag_run.conf['mapper_data']['timesheet_period']
                        },
                        "effectiveDate": dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']
                    }
                ]
    return [
        {
            "timesheetPeriod": {
            "uri": null,
            "name": dag_run.conf['mapper_data']['timesheet_period']
            },
            "effectiveDate": null
        }
    ]

def _get_timezone_to_apply(dag_run, exception_log):
    if dag_run.conf['timezone'].get('timezone_uri'):
        return {
            'uri': dag_run.conf['timezone'].get('timezone_uri'),
            'IANAName': null
        }
    exception_log.append(f"Time Zone not defined for country {dag_run.conf['file_data']['country']} in mapper")

    return {} 

def _get_policy_data_access_scope_to_assign():
    return  [
        {
            "policyUri": "urn:replicon:policy:time-off",
            "locations": [
                {
                    "location": null,
                    "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                    "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:include-descendants"
                }
            ],
            "divisions": [],
            "costCenters": [],
            "serviceCenters": [],
            "departmentGroups": [],
            "employeeTypeGroups": []
        },
        {
            "policyUri": "urn:replicon:policy:user",
            "locations": [
                {
                    "location": null,
                    "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
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

def _get_display_name_to_assign(dag_run):
    return {
        "displayName": f"""{dag_run.conf['file_data']['last_name']},{dag_run.conf['file_data']['first_name']} {dag_run.conf['file_data']['emp_id']} {dag_run.conf['file_data']['email_id']}"""
        }

def crete_user_payload(dag_run, config):
    exception_log = []
    payload = {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['file_data']['email_id'],
                "employeeId": null,
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf['file_data']['first_name'],
            "lastname": dag_run.conf['file_data']['last_name'],
            "emailAddress": _get_email_to_add(dag_run, config),
            "employeeId": dag_run.conf['file_data']['emp_id'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": _get_schedule_policy_to_assign(dag_run, config, exception_log),
            "workWeekStartDayUri": _get_work_week_to_assign(dag_run),
            "employmentDateRange": {
                "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    dag_run.conf['mapper_data']['authentication_uri']
                ],
                "isLoginEnabled": _get_is_login_enabled(dag_run),
                "loginName": dag_run.conf['file_data']['email_id'],
                "SSOName": dag_run.conf['file_data']['email_id'],
                "password": null
            },
            "holidayCalendar": _get_holiday_calendar_to_assign(dag_run),
            "holidayCalendarAssignmentSchedule": null,
            "timeOffPolicy": null,
            "permissionSets": _get_user_permission_to_assign(dag_run),
            "policySets": _get_policy_sets_to_assign(dag_run),
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": _get_timesheet_approval_path(dag_run),
            "expenseApprovalPath": null,
            "expenseDefaultReimbursementCurrency": null,
            "timeOffApprovalPath": _get_timeoff_approval_to_assign(dag_run),
            "workAuthorizationApprovalPath": null,
            "timeOffBalancePayoutApprovalPath": null,
            "customFieldValues": _get_custom_fields_to_assign(dag_run),
            "assignedActivities": _get_activity_list_to_assign(dag_run),
            "timeZone": _get_timezone_to_apply(dag_run, exception_log),
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": _get_location_to_assign(dag_run),
            "divisionSchedule": _get_division_to_assign(dag_run),
            "costCenterSchedule": _get_cost_center_to_apply(dag_run),
            "serviceCenterSchedule": _get_service_center_to_assign(dag_run),
            "departmentGroupSchedule": _get_department_to_assign(dag_run),
            "employeeTypeGroupSchedule": _get_employee_type_uri_to_assign(dag_run),
            "timesheetPeriodSchedule": _get_timesheet_period_schedule_to_assign(dag_run),
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": _get_policy_data_access_scope_to_assign(),
            "payRuleScriptSchedule": _get_payrule_to_assign(dag_run),
            "displayNameParameter": _get_display_name_to_assign(dag_run),
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": [],
            "workCompliancePolicyAssignmentSchedule": []
        }
    }

    rail.set_result(key="exception_log", val=exception_log)
    return payload

def get_notification_preference_to_assign():
    return {
        "user": {
            "uri": rail.result("create_user")["uri"]
        },
        "preferences": {
            "notificationDeliveryPreferences": [
                {
                    "objectTypeUri": "urn:replicon:object-type:project",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:user",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:timesheet",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:time-entry-revision-group",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:expense-sheet",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:time-off",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:holiday",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                }
            ],
            "sharedDeliveryPreferenceOptionUris": [
              "urn:replicon:user-shared-delivery-preference-option:always-deliver"
            ]
        }
    }


def get_product_to_assign_to_user_payload(dag_run):
    return {
        "userUri" : rail.result("create_user")["uri"],
        "productUris": dag_run.conf['mapper_data']['product_uri'].split('|')
    }

def get_update_time_entry_path_payload(dag_run):
    return {
        "user": {
            "uri": rail.result("create_user")["uri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "timeEntryRevisionGroupApprovalPathToApply": {
                "uri": null,
                "name": dag_run.conf['mapper_data']['time_entry_approval_path_name']
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def _can_update_first_name(dag_run, user_details):
    if dag_run.conf['file_data']['first_name']:
        return dag_run.conf['file_data']['first_name'] != user_details['firstName']
    return False

def _can_update_last_name(dag_run, user_details):
    if dag_run.conf['file_data']['last_name']:
        return dag_run.conf['file_data']['last_name'] != user_details['lastName']
    return False

# this will be called twice in update email and update displayValue
def _can_update_email(dag_run, user_details, config, login_name_check=True):
    if config.instance == "prod":
        if dag_run.conf['file_data']['email_id']:
            if login_name_check:
                return dag_run.conf['file_data']['email_id'] != user_details['securityConfiguration']['loginName']
            if not login_name_check:
                return (not user_details['userDetails']['emailAddress'])
    return False

def _can_update_display_name(dag_run, user_details, config):
    return _can_update_first_name(dag_run, user_details) or _can_update_last_name(dag_run, user_details)\
          or _can_update_email(dag_run, user_details, config, True) or _can_update_email(dag_run, user_details, config, False)


def _get_custom_fields_payload(uri, txt_value=null, date_value=null, drop_down_value_name=null, drop_down_value_uri=null, number_value=null):
    return {
        "customField": {
            "uri": uri,
            "name": null,
            "groupUri": null
        },
        "text": txt_value,
        "date": date_value,
        "dropDownOption": {
            "uri": drop_down_value_uri,
            "name": drop_down_value_name
            } if drop_down_value_name or drop_down_value_uri else null,
        "number": number_value
    }

def _update_txt_udf(dag_run, input_field_name, udf_display_text_value, udf_key_name, custom_fields_payload, current_custom_fields_values):
    # this field is derived by mapper
    if input_field_name == 'termination_reason_code':
        input_data = dag_run.conf['mapper_data']['termination_reason_code']
    elif input_field_name=="job_level" and dag_run.conf['file_data']['country'] == "Canada":
        input_data = f"H{dag_run.conf['file_data']['job_level']}"
    elif isinstance(input_field_name, list):
        input_data = rail.smartjoin_by_delim([dag_run.conf['file_data'][field_name] for field_name in input_field_name], separator='|')
    else:
        input_data = dag_run.conf['file_data'][input_field_name]
    if input_data:
        if input_data != rail.find_first_by_attr_and_get_attr(
            current_custom_fields_values, "customField.displayText", udf_display_text_value, 'text', default=""):
            custom_fields_payload.append(
                _get_custom_fields_payload(uri=dag_run.conf['udfs'][udf_key_name].get('uri'), txt_value=input_data))
            if input_field_name == "management_lvl":
                if dag_run.conf['file_data']['management_lvl'] in ['L1', 'L2']:
                    return True
                return False

def _get_work_shift_value(work_shift):
    if work_shift.startswith("BPSOT"):
        return "BPSOT"
    if work_shift.startswith("BPS"):
        return "BPS"
    return work_shift


def _update_date_udf(dag_run, input_field_name, json_formatted_date_field_name, udf_display_text_value, udf_key_name, custom_fields_payload, current_custom_fields_values):
    if dag_run.conf['file_data'][input_field_name]:
        if _compare_if_two_json_dates_are_same(date_1=dag_run.conf['json_formatted_dates'][json_formatted_date_field_name],
                                              date_2=rail.find_first_by_attr_and_get_attr(current_custom_fields_values,
                                                            "customField.displayText", udf_display_text_value, 'date', default="")):
            custom_fields_payload.append(
                _get_custom_fields_payload(
                    uri=dag_run.conf['udfs'][udf_key_name].get('uri'),
                    date_value=dag_run.conf['json_formatted_dates'][json_formatted_date_field_name]
                )
            )

def _update_drop_down_udf(dag_run, input_field_name, udf_display_text_value, udf_key_name, custom_fields_payload, current_custom_fields_values, custom_value_to_compare=null):
    if input_field_name == 'termination_reason_code':
        input_data = dag_run.conf['mapper_data']['termination_reason_code']
    elif isinstance(input_field_name, list):
        input_data = rail.smartjoin_by_delim([dag_run.conf['file_data'][field_name] for field_name in input_field_name], separator='|')
    # the value for work_shift is derived with a custom logic
    # to not create a new logic for that added below logic which will take care of the custom logic
    # without need of a new function
    elif input_field_name == "NA" and custom_value_to_compare:
        input_data = custom_value_to_compare
    else:
        input_data = dag_run.conf['file_data'][input_field_name]
    if input_data:
        if input_data != rail.find_first_by_attr_and_get_attr(
            current_custom_fields_values, "customField.displayText", udf_display_text_value, 'text', default=""):
            custom_fields_payload.append(
                    _get_custom_fields_payload(uri=dag_run.conf['udfs'][udf_key_name].get('uri'), drop_down_value_name=input_data))

def _update_custom_fields_for_user(dag_run):
    current_user_groups = rail.result("get_effective_group_membership")
    current_custom_fields_values = rail.result("get_user_details")['userDetails']['customFieldValues']
    custom_fields_payload = []
    exception_log = []
    _update_txt_udf(dag_run, 'assignment_type', 'assignment_type', 'assignment_type', custom_fields_payload, current_custom_fields_values)
    work_shift:str = dag_run.conf["file_data"]['work_shift']
    if work_shift:
        _update_drop_down_udf(dag_run, "NA", "Employee Group", 'employee_type_udf', custom_fields_payload, current_custom_fields_values, _get_work_shift_value(work_shift))
    _update_txt_udf(dag_run, 'work_shift', 'Work Shift', 'work_shift', custom_fields_payload, current_custom_fields_values)
    _update_date_udf(dag_run, 'dob', 'date_of_birth', 'Date of Birth', 'date_of_birth', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'middle_name', 'Middle Name', 'middle_name', custom_fields_payload, current_custom_fields_values)
    _update_drop_down_udf(dag_run, 'time_type', 'Time Type', 'time_type', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'perner_id', 'IA PERNER ID', 'ia_perner_id', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'gender', 'Gender', 'gender', custom_fields_payload, current_custom_fields_values)
    can_update_notifications_settings = _update_txt_udf(dag_run, 'management_lvl', 'Management Level', 'management_level', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'on_leave', 'On Leave', 'on_leave', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'area_code', 'Personnel Area Code', 'personnel_area_code', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'area_name', 'Personnel Area Description', 'personnel_area_name', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'job_level', 'Job Activity Type', 'job_level', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'fte', 'FTE', 'fte', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'fte_pct', 'FTE %', 'ftepct', custom_fields_payload, current_custom_fields_values)
    ia_updated, ia_exception_msg, effective_date = get_ia_update_payload_for_udf_update(dag_run, custom_fields_payload=custom_fields_payload, current_custom_fields_values=current_custom_fields_values,
                                                        update_txt_udf=_update_txt_udf, update_date_udf=_update_date_udf)
    rail.set_result(key="ia_updated", val=ia_updated)
    rail.set_result(key="ia_exception_msg", val=ia_exception_msg)
    _update_date_udf(dag_run, 'service_date', 'service_date', 'Continuous Service Date', 'service_date', custom_fields_payload, current_custom_fields_values)

    get_psa_udf_value(
        dag_run = dag_run,
        current_custom_fields_values = current_custom_fields_values,
        current_user_groups = current_user_groups,
        custom_fields_payload = custom_fields_payload,
        _get_custom_fields_payload = get_custom_fields_payload,
        _get_cost_center_update_payload = _get_cost_center_update_payload,
        _get_department_update_payload = _get_department_update_payload,
        caller= "update")

    # For IA=1 users, clear UDF fields that are not applicable to Portugal region
    udf_fields_to_clear = _get_udf_fields_to_clear_for_ia(dag_run, current_custom_fields_values, exception_log)
    custom_fields_payload.extend(udf_fields_to_clear)

    # Store exception log for later use
    if exception_log:
        existing_log = rail.result("ia_exception_msg") or []
        if isinstance(existing_log, str):
            existing_log = [existing_log] if existing_log else []
        rail.set_result(key="ia_exception_msg", val=existing_log + exception_log)

    rail.set_result("can_update_notifications_settings", can_update_notifications_settings)

    return custom_fields_payload, effective_date

def get_custom_fields_payload(uri, txt_value=null, date_value=null, drop_down_value_name=null, drop_down_value_uri=null, number_value=null):
    return {
        "customField": {
            "uri": uri,
            "name": null,
            "groupUri": null
        },
        "text": txt_value,
        "date": date_value,
        "dropDownOption": {
            "uri": drop_down_value_uri,
            "name": drop_down_value_name
            } if drop_down_value_name or drop_down_value_uri else null,
        "number": number_value
    }

def _get_two_date_diff(effective_date, user_start_date, today):
    if effective_date:
        return convert_json_date_to_date(today) - convert_json_date_to_date(effective_date)
    return convert_json_date_to_date(today) -  convert_json_date_to_date(user_start_date)


def _get_current_payrule_schedule_timesheetPeriod(payrule_schedule_details, user_start_date):
    current_effective_payrule = None
    # as an identifier to process very 1st record
    #! can be optimized
    current_min_day_diff = "*"
    today= get_todays_date_in_json()
    # iter from 2nd item as we have considered the 1st record as current
    for _schedule in payrule_schedule_details:
        day_diff_cnt = _get_two_date_diff(_schedule['effectiveDate'], user_start_date, today)

        # ignore the future ones
        if day_diff_cnt.days < 0:
            continue

        if current_min_day_diff=="*":
            current_effective_payrule = _schedule
            current_min_day_diff = day_diff_cnt
            continue

        if current_min_day_diff > day_diff_cnt:
            current_min_day_diff = day_diff_cnt
            current_effective_payrule = _schedule

    return current_effective_payrule

def _get_effective_date_based_on_work_week(work_week, work_week_starts_with_check:list):
    is_start_with_saturday = work_week.lower().split(" ")[0] == "saturday"
    today = datetime.now()
    if "saturday" in work_week_starts_with_check and is_start_with_saturday:
        # if today is saturday consider today as effective date
        if today.weekday() == 5:
            return today
        # for sunday we have to remove 1
        # for other days except for saturday we have to add 2
        # Monday= 0, Tuesday=1, .... Sunday=6
        if today.weekday() == 6:
            return today - timedelta(days=1)
        return today - timedelta(days=today.weekday()+2)
    if "sunday" in work_week_starts_with_check and is_start_with_saturday:
        # if today is saturday consider today as effective date
        if today.weekday() == 6:
            return today
        # for monday we have to remove 1
        # for other days except for saturday we have to add 1
        # Monday= 0, Tuesday=1, .... Sunday=6
        if today.weekday() == 0:
            return today - timedelta(days=1)
        return today - timedelta(days=today.weekday()+1)
    # if today is monday consider today as effective date
    if today.weekday() == 0:
        return today
    # Get the last immediate monday as effective date
    return today - timedelta(days=today.weekday())


def _get_payrule_schedule_to_update(dag_run, _get_user_details, effective_date):
    if dag_run.conf['payrule']['payrule']:
        current_payrule_schedule = _get_current_payrule_schedule_timesheetPeriod(_get_user_details['payRuleScriptSchedule'], _get_user_details['userDetails']['employmentDateRange']['startDate'])
        if not current_payrule_schedule or current_payrule_schedule['payRuleScript']['displayText'] != dag_run.conf['payrule']['payrule']:
            if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
                payrule_effective_date = _get_effective_date_based_on_work_week(dag_run.conf['mapper_data']['work_week'], ['saturday'])
                return {
                    "scheduleEntries": [
                        {
                            "payRuleScript": {
                                "uri" : null,
                                "name": dag_run.conf['payrule']['payrule']
                            },
                            "effectiveDate": effective_date if effective_date else {
                                "day": payrule_effective_date.day,
                                "month": payrule_effective_date.month,
                                "year": payrule_effective_date.year
                            }
                        }
                    ]
                }
            

def _get_shift_assignment_to_update(dag_run, user_details, config, exception_log, effective_date):
    current_office_schedule = _get_current_payrule_schedule_timesheetPeriod(user_details['schedulePolicies'], user_details['userDetails']['employmentDateRange']['startDate'])
    if dag_run.conf['schedule_data']['schedule_name'] == "Shift Schedule":
        mapper_shift_details = _get_shift_from_mapper(dag_run, config)
        if not mapper_shift_details:
              # to make sure it has a one element to avoid failure below
            mapper_shift_details = [{'URI': ''}]
        # added this for debugging purposes as without it it would be hard
        rail.set_result(key="mapper_shift_details", val=mapper_shift_details)
        if not current_office_schedule or (current_office_schedule['scheduleTypeUri'] != mapper_shift_details[0]['URI']):
            return {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementSchedule": [],
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": null,
                                "officeSchedule": null,
                                "scheduleTypeUri": mapper_shift_details[0]["URI"]
                            },
                            "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['work_shift_effective_date']
                        }
                    ],
                    "endDate": null
                }
            }
    
    else:
        if dag_run.conf['schedule_data']['schedule_name']:
            if not current_office_schedule or (not current_office_schedule['officeSchedule']) or (current_office_schedule['officeSchedule']['displayText'] != dag_run.conf['schedule_data']['schedule_name']):
                if dag_run.conf['schedule_data']['schedule_uri']:
                    return {
                        "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementSchedule": [],
                        "updateScheduleOverDateRange": {
                            "replacementScheduleEntries": [
                                {
                                    "schedulePolicy": {
                                        "officeScheduleUri": null,
                                        "name": dag_run.conf['schedule_data']['schedule_name'],
                                        "officeSchedule": {
                                            "officeScheduleUri": null,
                                            "name": dag_run.conf['schedule_data']['schedule_name']
                                        },
                                        "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri']
                                    },
                                    "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['work_shift_effective_date']
                                }
                            ],
                            "endDate": null
                        }
                    }
                else:
                    exception_log.append(f"""Office schedule {dag_run.conf['schedule_data']['schedule_name']} not available in Replicon""")

    return null


def _get_time_entry_approval_path_name(dag_run):
    current_timeentry_approval_path = rail.result("get_time_entry_approval_path")
    if not current_timeentry_approval_path or current_timeentry_approval_path['displayText'] != dag_run.conf['mapper_data']['time_entry_approval_path_name']:
        return {
            "uri": null,
            "name":  dag_run.conf['mapper_data']['time_entry_approval_path_name']
        }
    return null

def _can_update_timesheet_template(dag_run, user_details):
    if dag_run.conf['mapper_data']['timesheet_template']:
        timesheet_template = user_details['timesheetTemplate']['name'] if user_details['timesheetTemplate'] else ''
        if (not timesheet_template) or (timesheet_template != dag_run.conf['mapper_data']['timesheet_template']):
            return True
    return False

def _can_update_timeoff_template(dag_run, user_details):
    if dag_run.conf['mapper_data']['timeoff_template']:
        timeoff_template = user_details['timeOffTemplate'].get('name', '') if user_details['timeOffTemplate'] else ''
        if (not user_details['timeOffTemplate'].get('name', '')) or (timeoff_template != dag_run.conf['mapper_data']['timeoff_template']):
            return True
    return False

def _can_update_punch_entry_policies(dag_run):
    if not  dag_run.conf['policy_sets'].get('punch_entry_policy', {}).get('name', ''):
        return False
    current_assigned_policies = rail.result("get_user_assigned_policy")
    return not bool(list(filter(lambda time_punch_policy: time_punch_policy['policySet']['name'] == dag_run.conf['policy_sets']['punch_entry_policy'].get('name', ''), 
                filter(lambda policy: policy['policyUri']=="urn:replicon:policy:time-punch", current_assigned_policies))))


def _get_policies_to_update_payload(dag_run, user_details, exception_log):
    policies_to_add = []
    if _can_update_punch_entry_policies(dag_run):
        policies_to_add.append(dag_run.conf['policy_sets']['punch_entry_policy']['uri'])

    if _can_update_timesheet_template(dag_run, user_details):
        if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']: 
            if dag_run.conf['policy_sets']['timesheet_template'].get('uri'):
                policies_to_add.append(dag_run.conf['policy_sets']['timesheet_template']['uri'])
            else:
                exception_log.append(f"Timesheet template {dag_run.conf['mapper_data']['timesheet_template']} not available in Replicon")

    if _can_update_timeoff_template(dag_run, user_details):
        if dag_run.conf['policy_sets']['timeoff_template'].get('uri'):
            policies_to_add.append(dag_run.conf['policy_sets']['timeoff_template']['uri'])
        else:
            exception_log.append(f"Timesheet template {dag_run.conf['mapper_data']['timesheet_template']} not available in Replicon")

    return {
                "policySetUrisToAssign": policies_to_add,
                "policyUrisToRemovePolicySet": [],
                "policySetUrisToRemove": []
            } if policies_to_add else null


def _get_timesheet_period_schedule_to_apply(dag_run, user_details, effective_date):
    if user_details['timesheetPeriodSchedule']:
        current_timesheet_period = _get_current_payrule_schedule_timesheetPeriod(user_details['timesheetPeriodSchedule'],
                                                                                 user_details['userDetails']['employmentDateRange']['startDate'])
        timesheet_effective_date = _get_effective_date_based_on_work_week(dag_run.conf['mapper_data']['work_week'], ['saturday', 'sunday'])
        if not current_timesheet_period and dag_run.conf['mapper_data']['timesheet_period']:
            return {
                "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementTimesheetPeriodSchedule": [],
                "updateTimesheetPeriodScheduleOverDateRange": {
                    "replacementTimesheetPeriodScheduleEntries": [
                        {
                            "timesheetPeriod": {
                                "uri": null,
                                "name": dag_run.conf['mapper_data']['timesheet_period']
                            },
                            "effectiveDate": effective_date if effective_date else {
                                "day": timesheet_effective_date.day,
                                "month": timesheet_effective_date.month,
                                "year": timesheet_effective_date.year
                            }
                        }
                    ],
                    "endDate": null
                }
            }

        if dag_run.conf['mapper_data']['timesheet_period'] and (not current_timesheet_period or dag_run.conf['mapper_data']['timesheet_period'] != current_timesheet_period['timesheetPeriod']['displayText']):
            return {
                "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementTimesheetPeriodSchedule": [],
                "updateTimesheetPeriodScheduleOverDateRange": {
                    "replacementTimesheetPeriodScheduleEntries": [
                        {
                            "timesheetPeriod": {
                                "uri": null,
                                "name": dag_run.conf['mapper_data']['timesheet_period']
                            },
                            "effectiveDate": effective_date if effective_date else {
                                "day": timesheet_effective_date.day,
                                "month": timesheet_effective_date.month,
                                "year": timesheet_effective_date.year
                            }
                        }
                    ],
                    "endDate": null
                }
            }
    return null

def _get_timesheet_period_to_apply_profile_status_enabled(dag_run, user_details):
    # user_details['timesheetPeriodSchedule'] will be blank if there is no value assigned to the user
    if not user_details['timesheetPeriodSchedule']:
        if dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']:
            if convert_json_date_to_date(dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']) > convert_json_date_to_date(
                        dag_run.conf['json_formatted_dates']['hire_date']):
                return {
                    "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementTimesheetPeriodSchedule": [],
                    "updateTimesheetPeriodScheduleOverDateRange": {
                        "replacementTimesheetPeriodScheduleEntries": [
                            {
                                "timesheetPeriod": {
                                    "uri": null,
                                    "name": dag_run.conf['mapper_data']['timesheet_period']
                                },
                                "effectiveDate": dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']
                            }
                        ],
                        "endDate": null
                    }
                }
            if convert_json_date_to_date(dag_run.conf['json_formatted_dates']['hire_date']) > convert_json_date_to_date(
                        dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']):
                return {
                    "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementTimesheetPeriodSchedule": [],
                    "updateTimesheetPeriodScheduleOverDateRange": {
                        "replacementTimesheetPeriodScheduleEntries": [
                            {
                                "timesheetPeriod": {
                                    "uri": null,
                                    "name": dag_run.conf['mapper_data']['timesheet_period']
                                },
                                "effectiveDate": null
                            }
                        ],
                        "endDate": null
                    }
                }
            if not dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']:
                return {
                    "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementTimesheetPeriodSchedule": [],
                    "updateTimesheetPeriodScheduleOverDateRange": {
                        "replacementTimesheetPeriodScheduleEntries": [
                            {
                                "timesheetPeriod": {
                                    "uri": null,
                                    "name": dag_run.conf['mapper_data']['timesheet_period']
                                },
                                "effectiveDate": null
                            }
                        ],
                        "endDate": null
                    }
                }
    
    return null

def _get_timesheetperiod_update_payload(dag_run, user_details, effective_date):
    if not user_details['timesheetPeriodSchedule'] and dag_run.conf['mapper_data']['profile_status'].lower() == "enabled":
        return _get_timesheet_period_to_apply_profile_status_enabled(dag_run, user_details)
    return _get_timesheet_period_schedule_to_apply(dag_run, user_details, effective_date)


def _get_department_update_payload(dag_run, current_effective_grps, effective_date):
    if dag_run.conf['file_data']['org_code'] and dag_run.conf['file_data']['org_code'] != current_effective_grps['department'].get('displayText', ''):
        return {
            "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementDepartmentGroupSchedule": [],
            "updateDepartmentGroupScheduleOverDateRange": {
                "replacementDepartmentGroupScheduleEntries": [
                    {
                        "departmentGroup": {
                            "uri": dag_run.conf['groups']['department'].get('uri'),
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['work_week_date']
                    }
                ],
                "endDate": null
            }
        }
    
    return null


def _get_location_update_payload(dag_run, current_effective_grps, effective_date):
    if dag_run.conf['groups']['location'] and dag_run.conf['groups']['location'].get('uri' ,'') != current_effective_grps['location'].get('uri' ,''):
        return {
            "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementLocationSchedule": [],
            "updateLocationScheduleOverDateRange": {
                "replacementLocationScheduleEntries": [
                    {
                        "location": {
                            "uri": dag_run.conf['groups']['location'].get('uri'),
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['location_effective_date']
                    }
                ],
                "endDate": null
            }
        }

    return null


def _get_cost_center_update_payload(dag_run, current_effective_grps, effective_date):

    if dag_run.conf['file_data']['cost_center'] and dag_run.conf['file_data']['cost_center'] != current_effective_grps['costCenter'].get('displayText', ''):
        return {
            "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementCostCenterSchedule": [],
            "updateCostCenterScheduleOverDateRange": {
                "replacementCostCenterScheduleEntries": [
                    {
                        "costCenter": {
                            "uri": dag_run.conf['groups']['cost_center'].get('uri' ,''),
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['cost_center_effective_date']
                    }
                ],
                "endDate": null
            }
        }
    
    return null


def _get_division_update_payload(dag_run, current_effective_grps, effective_date):
    if dag_run.conf['groups']['division'].get('uri' ,'') and dag_run.conf['groups']['division'].get('uri' ,'') != current_effective_grps['division'].get('uri' ,''):
        return {
            "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementDivisionSchedule": [],
            "updateDivisionScheduleOverDateRange": {
                "replacementDivisionScheduleEntries": [
                    {
                        "division": {
                            "uri": dag_run.conf['groups']['division'].get('uri' ,''),
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": effective_date if effective_date else (dag_run.conf['json_formatted_dates']['job_change_effective_date'] if dag_run.conf[
                            'file_data']['job_change_effective_date'] else dag_run.conf['json_formatted_dates']['cost_center_effective_date'])
                    }
                ],
                "endDate": null
            }
        }

    return null

def _get_employee_type_update_payload(dag_run, current_effective_grps, effective_date):
    if dag_run.conf['groups']['employee_type'] and dag_run.conf['groups']['employee_type']['uri'] and (dag_run.conf['groups']['employee_type']['uri'] != current_effective_grps['employeeType'].get('uri', '')):
        return {
            "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementEmployeeTypeGroupSchedule": [],
            "updateEmployeeTypeGroupScheduleOverDateRange": {
                "replacementEmployeeTypeGroupScheduleEntries": [
                    {
                        "employeeTypeGroup": {
                            "uri": dag_run.conf['groups']['employee_type']['uri'],
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['employee_type_effective_date']
                    }
                ],
                "endDate": null
            }
        }
    
    return null


def _get_service_center_update_payload(dag_run, current_effective_grps, effective_date):
    if dag_run.conf['file_data']['pay_group'] and dag_run.conf['file_data']['pay_group'] != current_effective_grps['serviceCenter'].get('displayText', ''):
        return {
            "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementServiceCenterSchedule": [],
            "updateServiceCenterScheduleOverDateRange": {
                "replacementServiceCenterScheduleEntries": [
                {
                    "serviceCenter": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf['file_data']['pay_group']
                    },
                    "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['work_week_date']
                }
                ],
                "endDate": null
            }
        }
    
    return null

def _get_timezone_update_payload(dag_run, user_details, logger):
    if dag_run.conf['timezone']['timezone']:
        if dag_run.conf['timezone']['timezone_uri'] != user_details['timeZone']['uri']:
            return {
                "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": {
                    "uri": dag_run.conf['timezone']['timezone_uri'],
                    "IANAName": null
                }
            }
        return null
    else:
        logger.append(f"Timezone not defined in mapper for Location {dag_run.conf['file_data']['country']}")
    return null


def _get_work_week_update_payload(dag_run, profile_status_is_enabled):
    if profile_status_is_enabled:
        if dag_run.conf['mapper_data']['work_week_uri']:
            return {
                "workWeekStartDayUri": dag_run.conf['mapper_data']['work_week_uri']
            }
    return null

def _get_timeoff_approval_update_payload(dag_run, profile_status_is_enabled):
    if profile_status_is_enabled:
        if dag_run.conf['mapper_data']['timeoff_approval']:
            return {
                "uri": null,
                "name": dag_run.conf['mapper_data']['timeoff_approval']
            }
    return null

def _get_holiday_calendar_update_payload(dag_run, user_details, profile_status_is_enabled, exception_log):
    if profile_status_is_enabled:
        holiday_calendar = user_details['holidayCalendar'].get('displayText', '') if rail.result('get_user_details')['holidayCalendar'] else ''
        if dag_run.conf['mapper_data']['holiday_calendar']:
            if dag_run.conf['mapper_data']['holiday_calendar'] != holiday_calendar:
                if dag_run.conf['mapper_data']['holiday_calendar_uri']:
                    return {
                        "holidayCalendar": {
                            "uri": dag_run.conf['mapper_data']['holiday_calendar_uri']['uri'],
                            "name": null
                        }
                    }
                exception_log.append(f''''Holiday calendar "{dag_run.conf['mapper_data']['holiday_calendar']}" not available in Replicon''')
    return null

def _get_activities_update_payload(dag_run, user_details):
    if dag_run.conf['activities']['activity']:
        activity_list = dag_run.conf['activities']['activity'].split('|')
        user_activities = user_details['assignedActivities']
        can_assign_activities = False
        for activity in user_activities:
            if activity['name'] not in activity_list:
                can_assign_activities = True
                break
        if can_assign_activities:
            return list(map(lambda _activity: {
                    "uri": null,
                    "name": _activity,
                }, activity_list))
    return null

def get_update_user_payload(dag_run, config):
    exceptions = []
    user_details = rail.result("get_user_details")
    current_user_groups = rail.result("get_effective_group_membership")
    profile_status_is_enabled = dag_run.conf['mapper_data']['profile_status'] == "enabled"

    custom_fields, effective_date = _update_custom_fields_for_user(dag_run)

    payload = {
        "user": {
            "uri": dag_run.conf['user_uri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "timezoneToApply": _get_timezone_update_payload(dag_run, user_details, exceptions),
            "workWeekStartToApply": _get_work_week_update_payload(dag_run, profile_status_is_enabled),
            "holidayCalendarToApply": _get_holiday_calendar_update_payload(dag_run, user_details, profile_status_is_enabled, exceptions),
            "holidayCalendarAssignmentsToApply": null,
            "schedulePolicyToApply": _get_shift_assignment_to_update(dag_run, user_details, config, exceptions, effective_date),
            "locationScheduleToApply": _get_location_update_payload(dag_run, current_user_groups, effective_date),
            "divisionScheduleToApply": _get_division_update_payload(dag_run, current_user_groups, effective_date),
            "costCenterScheduleToApply": _get_cost_center_update_payload(dag_run, current_user_groups, effective_date),
            "departmentGroupScheduleToApply": _get_department_update_payload(dag_run, current_user_groups, effective_date),
            "employeeTypeGroupScheduleToApply": _get_employee_type_update_payload(dag_run, current_user_groups, effective_date),
            "timesheetPeriodScheduleToApply": _get_timesheetperiod_update_payload(dag_run, user_details, effective_date),
            "serviceCenterScheduleToApply": _get_service_center_update_payload(dag_run, current_user_groups, effective_date),
            "totalBusinessCostScheduleToApply": null,
            "permissionSetsToApply": null,
            "policySetsToApply": _get_policies_to_update_payload(dag_run, user_details, exceptions),
            "policyDataAccessScopesToApply": null,
            "policyDataAccessScopesToApply2": null,
            "notificationPreferencesToApply": null,
            "timesheetPeriodTypeToApply": null,
            "timesheetApprovalPathToApply": {
                "uri": null,
                "name": dag_run.conf['mapper_data']['timesheet_approval_path']
            },
            "timeEntryRevisionGroupApprovalPathToApply": _get_time_entry_approval_path_name(dag_run),
            "validationRuleToApply": null,
            "activitiesToApply": _get_activities_update_payload(dag_run, user_details),
            "defaultActivityToApply": null,
            "defaultActivityToApply2": null,
            "defaultTimeOffTypeForBookingsToApply": null,
            "expenseApprovalPathToApply": null,
            "expenseDefaultReimbursementCurrencyToApply": null,
            "timeOffApprovalPathToApply": _get_timeoff_approval_update_payload(dag_run, profile_status_is_enabled),
            "productAssignmentsToApply": null,
            "timeBankPolicyToApply": null,
            "securitySettingsToApply": null,
            "supervisorsToApply": null,
            "supervisorsModifications": null,
            "payrollRatesToApply": null,
            "payrollRatesModifications": null,
            "overtimeRulesToApply": null,
            "overtimeRulesModifications": null,
            "customFieldValuesToApply": custom_fields,
            "departmentToApply": null,
            "employeeTypeToApply": null,
            "userDetailsToApply": {
                "firstName": dag_run.conf['file_data']['first_name'] if _can_update_first_name(dag_run, user_details['userDetails']) else null,
                "lastName": dag_run.conf['file_data']['last_name'] if _can_update_last_name(dag_run, user_details['userDetails']) else null,
                "emailAddress": {
                    "emailAddress": dag_run.conf['file_data']['email_id']
                } if _can_update_email(dag_run, user_details['userDetails'], config) or _can_update_email(dag_run, user_details['userDetails'], config,False) else null,
                "language": null,
                "employmentDateRange": null,
                "employmentStartDate": null,
                "employmentEndDate": null,
                "employeeId": null,
                "displayNameParameter": _get_display_name_to_assign(dag_run)
            } if _can_update_display_name(dag_run, user_details['userDetails'], config) else null,
            "payRulesToApply": null,
            "payRulesScheduleModifications": _get_payrule_schedule_to_update(dag_run, user_details, effective_date),
            "payRatesModifications": null,
            "placeAssignmentsModifications": null,
            "resourceAllocationAfterUserEndDateOptionUri": null,
            "projectRolesToApply": null,
            "projectRoleAssignmentSchedulesToApply": null,
            "decimalSeparatorToApply": null,
            "numberGroupSeparatorToApply": null,
            "dateFormatToApply": null,
            "clockFormatToApply": null,
            "hoursFormatToApply": null,
            "timeZoneFormatToApply": null,
            "objectExtensionFieldsToApply": [],
            "costRateScheduleModifications": null,
            "workAuthorizationApprovalPathToApply": null,
            "displayNameFormatSettingsToApply": null,
            "timePunchTimeZoneDisplayOptionToApply": null,
            "defaultTimesheetToDisplayOptionToApply": null,
            "reportSettingsToApply": null,
            "timeOffBalancePayoutApprovalPathToApply": null,
            "workCompliancePolicyAssignmentScheduleToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }
    

    rail.set_result(key="exception_log", val=exceptions)
    return payload
