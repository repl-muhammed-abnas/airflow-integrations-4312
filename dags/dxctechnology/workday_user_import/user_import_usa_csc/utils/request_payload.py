from datetime import datetime
from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import convert_json_date_to_date
from dxctechnology.workday_user_import.user_import.common_utils.request_payload import get_todays_date_in_json
import rail


null = None

INPUT_DATE_FORMAT = "%Y-%d-%m"

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
    if config.instance in ["prod", "production","trial"]:
        return dag_run.conf['file_data']['email_id']
    return null

def _get_user_permission_to_assign(dag_run):
    if dag_run.conf['user_permission_sets']['end_user_permission']:
        return [
            {
                "uri": dag_run.conf['user_permission_sets']['end_user_permission'],
                "name": null
            }
        ]
    return []

def _get_shift_from_mapper(dag_run, config):
    country = dag_run.conf['file_data']['country']
    parent_company = dag_run.conf['file_data']['parent_company']
    psg = dag_run.conf['mapper_data']['psg']
    employee_group = dag_run.conf['file_data']['emp_group_code']
    employee_sub_group = dag_run.conf['file_data']['emp_subgroup_code']
    status = dag_run.conf['file_data']['sub_area_code']
    return list(filter(lambda row: row['Type'] == "Schedule Type" and
                        row['Function'] == "Workday User Sync" and
                        row['Country'] == country and
                        row['Source'] == parent_company and
                        row['personnelsubarea'] == psg and
                        row['employeegroup'] == employee_group and
                        row['employeesubgroup'] == employee_sub_group and
                        row['status'] == status, config.MAPPER))

def _get_schedule_policy_to_assign(dag_run, config, exception_log):
    shift_from_mapper = _get_shift_from_mapper(dag_run, config)
    if shift_from_mapper:
        return [
            {
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": shift_from_mapper[0]['URI']
                },
                "effectiveDate": null
            }
        ]
    else:
        work_shift = dag_run.conf['file_data']['work_shift']
        if work_shift:
            if dag_run.conf['schedule_data']['schedule_uri']:
                return [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": work_shift,
                            "officeSchedule": {
                                "officeScheduleUri": null,
                                "name": work_shift
                            },
                            "scheduleTypeUri": dag_run.conf['mapper_data']['schedule_type_uri']
                        },
                        "effectiveDate": null
                    }
                ]
            else:
                exception_log.append(f"""Office schedule {work_shift} not available in Replicon. Hence default shift assigned""")
                return [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf['schedule_data']['office_schedule'],
                            "officeSchedule": {
                                "officeScheduleUri": null,
                                "name": dag_run.conf['schedule_data']['office_schedule']
                            },
                            "scheduleTypeUri": dag_run.conf['mapper_data']['schedule_type_uri']
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
                            "scheduleTypeUri": dag_run.conf['mapper_data']['schedule_type_uri']
                        },
                        "effectiveDate": null
                    }
                ]
        return null

def _get_policy_sets_to_assign(dag_run):
    policy_sets = []
    if dag_run.conf['mapper_data']['profile_status'] in ['true', True, 'True'] or dag_run.conf['mapper_data']['profile_status'] == "enabled":
        if dag_run.conf['policy_sets']['timeoff_template']['timeoff_template']:
            policy_sets.append({
                "name": dag_run.conf['policy_sets']['timeoff_template']['timeoff_template'],
                "uri": null
            })

        if dag_run.conf['mapper_data']['c1_timesheet_template'] and dag_run.conf['mapper_data']['profile_status']:
            if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
                policy_sets.append({
                    "name": dag_run.conf['mapper_data']['c1_timesheet_template'],
                    "uri": null
                })
        
        if dag_run.conf['policy_sets']['punch_entry_policy']['punch_entry_policy']:
            policy_sets.append({
                "uri": null,
                "name": dag_run.conf['policy_sets']['punch_entry_policy']['punch_entry_policy']
            })

    return policy_sets

def _get_timesheet_approval_path(dag_run):
    if dag_run.conf['mapper_data']['timesheet_approval_c1']:
        return {
            "uri": null,
            "name": dag_run.conf['mapper_data']['timesheet_approval_c1']
        }

    return null

def _get_timeoff_approval_to_assign(dag_run):
    if dag_run.conf['mapper_data']['timeoff_approval_c1']:
        return {
            "uri": null,
            "name": dag_run.conf['mapper_data']['timeoff_approval_c1']
        }

    return null


def _get_work_week_to_assign(dag_run):
    if dag_run.conf['mapper_data']['work_week_uri']:
        return dag_run.conf['mapper_data']['work_week_uri']
    return null 


def _get_holiday_calendar_to_assign(dag_run):
    if dag_run.conf['mapper_data']['holiday_calendar']:
        return {
            "uri": dag_run.conf['holiday_calendar']['holiday_calendar_uri'],
            "name": null
        }
    return null

def _get_employee_type_uri_to_assign(dag_run):
    if dag_run.conf['groups']['employee_type']['employee_type_uri_for_all']:
        return [
            {
                "employeeTypeGroup": {
                    "uri": dag_run.conf['groups']['employee_type']['employee_type_uri_for_all'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }
        ]
    return []

def _get_payrule_from_mapper(dag_run, config, use_state=False, use_emp_sub_grp_code = True):
    country = dag_run.conf['file_data']['country']
    parent_company = dag_run.conf['file_data']['parent_company']
    work_shift = dag_run.conf['file_data']['work_shift']
    emp_sub_group_code = dag_run.conf['file_data']['emp_subgroup_code']
    if use_state:
        state = dag_run.conf['file_data']['state']
        return list(filter(lambda row: row['Type'] == "Limited Payrule" and
                        row['Function'] == "Workday User Sync" and
                        row['Country'] == country and
                        row['Source'] == parent_company and
                        row['URI'] == work_shift and 
                        row['status'] == state and
                        ((row['employeesubgroup'] == emp_sub_group_code) if use_emp_sub_grp_code else True), config.MAPPER))
    return list(filter(lambda row: row['Type'] == "Limited Payrule" and
                        row['Function'] == "Workday User Sync" and
                        row['Country'] == country and
                        row['Source'] == parent_company and
                        row['URI'] == work_shift and
                        ((row['employeesubgroup'] == emp_sub_group_code) if use_emp_sub_grp_code else True), config.MAPPER))


def _get_payrule_to_assign(dag_run, config):
    if dag_run.conf['payrule']['payrule']:
        if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
            if dag_run.conf['file_data']['work_shift'] and dag_run.conf['file_data']['parent_company'] == "C1":
                if dag_run.conf['file_data']['country'] == "Puerto Rico":
                    payrule_from_mapper = _get_payrule_from_mapper(dag_run, config)
                    if payrule_from_mapper:
                        return [
                            {
                                "payRuleScript": {
                                    "uri": null,
                                    "name": payrule_from_mapper[0]['Value']
                                },
                                "effectiveDate": null
                            }
                        ]
                    return [
                            {
                                "payRuleScript": {
                                    "uri": null,
                                    "name": dag_run.conf['payrule']['payrule']
                                },
                                "effectiveDate": null
                            }
                        ]
                else:
                    if dag_run.conf['file_data']['country'] == "United States of America" and dag_run.conf['file_data']['state'] == "Puerto Rico":
                        payrule_from_mapper = _get_payrule_from_mapper(dag_run, config, True)
                        if payrule_from_mapper:
                            return [
                                {
                                    "payRuleScript": {
                                        "uri": null,
                                        "name": payrule_from_mapper[0]['Value']
                                    },
                                    "effectiveDate": null
                                }
                            ]
                        return []
                    else:
                        return [
                            {
                                "payRuleScript": {
                                    "uri": null,
                                    "name": dag_run.conf['payrule']['payrule']
                                },
                                "effectiveDate": null
                            }
                        ]
            else:
                return [
                            {
                                "payRuleScript": {
                                    "uri": null,
                                    "name": dag_run.conf['payrule']['payrule']
                                },
                                "effectiveDate": null
                            }
                        ]
    else:
        if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
            if dag_run.conf['file_data']['work_shift'] and dag_run.conf['file_data']['parent_company'] == "C1":
                if dag_run.conf['file_data']['country'] == "Puerto Rico":
                    payrule_from_mapper =  _get_payrule_from_mapper(dag_run, config, False, False)
                    if payrule_from_mapper:
                        return [
                            {
                                "payRuleScript": {
                                    "uri": null,
                                    "name": payrule_from_mapper[0]['Value']
                                },
                                "effectiveDate": null
                            }
                        ]
                    return []
                else:
                    if dag_run.conf['file_data']['country'] == "United States of America" and dag_run.conf['file_data']['state'] == "Puerto Rico":
                        payrule_from_mapper =  _get_payrule_from_mapper(dag_run, config, True, False)
                        if payrule_from_mapper:
                            return [
                                {
                                    "payRuleScript": {
                                        "uri": null,
                                        "name": payrule_from_mapper[0]['Value']
                                    },
                                    "effectiveDate": null
                                }
                            ]
    return []


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
    if not dag_run.conf['activities']['c1_activity']:
        return []
    activity_list = dag_run.conf['activities']['c1_activity'].split("|")
    
    return list(map(lambda activity: {
        "uri": null,
        "name": activity
    }, activity_list))

def _get_is_login_enabled(dag_run):
    if (not dag_run.conf['allowed_country']) or (dag_run.conf['allowed_country'].lower() != "enable") or\
        (not dag_run.conf['file_data']['parent_company']) or (not dag_run.conf['mapper_data']['profile_status']) or\
            (dag_run.conf['mapper_data']['profile_status'] != "enabled"):
        return False
    if str(dag_run.conf['file_data']['on_leave']) == '1':
        return False
    return dag_run.conf['replicon_field']

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
        udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_start_date']['uri'], date=dag_run.conf['today']))
    
    if dag_run.conf['file_data']['ia_end_date']:
        udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_end_date']['uri'], date=get_replicon_date(dag_run.conf['file_data']['ia_end_date'])))

    if dag_run.conf['file_data']['is_ia']:
        udfs_to_assign.append(_add_custom_field(custom_fields['ee_group']['uri'], text=dag_run.conf['file_data']['emp_group_code']))

    return udfs_to_assign

def _get_timezone_to_apply(dag_run, exception_log):
    if dag_run.conf['timezone'].get('timezone_uri'):
        return {
            'uri': dag_run.conf['timezone'].get('timezone_uri'),
            'IANAName': null
        }
    exception_log.append(f"Time Zone not defined for country {dag_run.conf['file_data']['country']} in mapper")

    return null

def _get_timesheet_period_schedule_to_assign(dag_run):
    if dag_run.conf['mapper_data']['timesheet_period_c1']:
        if dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']:
            if convert_json_date_to_date(dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']) >\
                convert_json_date_to_date(get_todays_date_in_json()):
                    return [
                        {
                            "timesheetPeriod": {
                                "uri": null,
                                "name": dag_run.conf['mapper_data']['timesheet_period_c1']
                            },
                            "effectiveDate": dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']
                        }
                    ]
        return [
            {
                "timesheetPeriod": {
                "uri": null,
                "name": dag_run.conf['mapper_data']['timesheet_period_c1']
                },
                "effectiveDate": null
            }
        ]

    return []

def _get_display_name_to_assign(dag_run):
    return {
        "displayName": f"""{dag_run.conf['file_data']['last_name']},{dag_run.conf['file_data']['first_name']} {dag_run.conf['file_data']['emp_id']} {dag_run.conf['file_data']['email_id']}"""
        }

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
            "payRuleScriptSchedule": _get_payrule_to_assign(dag_run,config),
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
        "productUris": dag_run.conf['mapper_data']['product_uri']
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
                "name": dag_run.conf['mapper_data']['timeentry_approval_path_name']
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
