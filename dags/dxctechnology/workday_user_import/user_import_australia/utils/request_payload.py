from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dxctechnology.workday_user_import.user_import.common_utils.custom_methods \
    import convert_json_date_to_date, compare_two_dates, get_work_week_based_effective_date, get_ia_update_payload_for_udf_update, compare_if_two_json_dates_are_same
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

# schedulePolicySchedule
def _get_shift_to_assign_payload_for_aus_user(dag_run, exception_log):
    # updated the same in the payrule function
    if dag_run.conf['schedule_data']['schedule_name'].lower() == 'shift':
        return [
            {
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri'] # urn:replicon:schedule-type:shift
                },
                "effectiveDate": null
            }
        ]
    if dag_run.conf['schedule_data']['schedule_name']:
        if dag_run.conf['schedule_data']['schedule_uri']:
            return [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": dag_run.conf['schedule_data']['schedule_uri'], # office_schedule_uri
                        "name": null,
                        "officeSchedule": {
                            "officeScheduleUri": dag_run.conf['schedule_data']['schedule_uri'], # office_schedule_uri
                            "name": null
                        },
                        "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri'], # office_schedule_name
                    },
                    "effectiveDate": null
                }
            ]
        else:
            exception_log.append(f"Office schedule '#schedule_name#' not available in Replicon. Hence default shift assigned")
            return [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": dag_run.conf['schedule_data']['office_schedule'], # office_schedule_name
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf['schedule_data']['office_schedule'], # office_schedule_name
                        },
                        "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri'], # office_schedule_name
                    },
                    "effectiveDate": null
                }
            ]
    else:
        if 'office_schedule':
            return [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": dag_run.conf['schedule_data']['office_schedule'], # office_schedule_name
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf['schedule_data']['office_schedule'] #office_schedule_name
                        },
                        "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri'] # schedule_type_uri
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

# permissionSets
def _get_end_user_permission_to_assign_payload_for_aus_user(dag_run):
    if dag_run.conf['user_permission_sets']['end_user_permission'].get('uri') and dag_run.conf['file_data']['area_code'] != 'AU36':
        return [
            {
                "uri": dag_run.conf['user_permission_sets']['end_user_permission']['uri'],
                "name": null
            }
        ]
    if dag_run.conf['file_data']['area_code'] == 'AU36' and dag_run.conf['file_data']['company_code'] == '3124':
        return [
            {
                "uri": null,
                "name": dag_run.conf['mapper_data']['end_user_permission_connect_emp']
            }
        ]
    return []

# policySets
def _get_policies_to_assign_payload_for_aus_user(dag_run):
    policy_sets = []
    if dag_run.conf['mapper_data']['timesheet_template'] and dag_run.conf['mapper_data']['profile_status'].lower() == 'enabled':
        policy_sets.append({
        "uri": null,
        "name": dag_run.conf['mapper_data']['timesheet_template']
      })
    if dag_run.conf['mapper_data']['timeoff_template'] and dag_run.conf['mapper_data']['profile_status'].lower() == 'enabled':
        policy_sets.append({
        "uri": null,
        "name": dag_run.conf['mapper_data']['timeoff_template']
      })
    return policy_sets

# timesheetApprovalPath
def _get_timesheet_approval_path_payload_for_aus_user(dag_run):
    if not dag_run.conf['mapper_data']['timesheet_approval_path']:
        return null
    return {
      "uri": null,
      "name": dag_run.conf['mapper_data']['timesheet_approval_path']
    }

# timeOffApprovalPath
def _get_timeoff_approval_path_payload_for_aus_user(dag_run):
    if not dag_run.conf['mapper_data']['timeoff_approval']:
        return null
    return {
      "uri": null,
      "name": dag_run.conf['mapper_data']['timeoff_approval']
    }

# workWeekStartDayUri
def _get_work_week_payload_for_aus_user(dag_run):
    if not dag_run.conf['mapper_data']['work_week_uri']:
        return null
    return dag_run.conf['mapper_data']['work_week_uri']

# holidayCalendar
def _get_holiday_calendar_payload_for_aus_user(dag_run):
    if not dag_run.conf['holiday_calendar'].get('holiday_calendar_uri'):
        return null
    return {
      "name": null,
      "uri": dag_run.conf['holiday_calendar']['holiday_calendar_uri']['uri']
    }

# employeeTypeGroupSchedule
def _get_employee_type_payload_for_aus_user(dag_run):
    if not dag_run.conf['groups']['employee_type'].get('uri'):
        return []
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

# payRuleScriptSchedule
def _get_payrule_script_schedule_payload_for_aus_user(dag_run, config):
    if dag_run.conf['file_data']['industrial_instrument_classification'] and \
            dag_run.conf['schedule_data']['schedule_name'] == 'shift' and \
            'BFI' in dag_run.conf['file_data']['industrial_instrument_classification']:
        payrule_value = list(filter(lambda row: row['Type'] == "Payrule" and
                                row['Function'] == "Workday User Sync" and
                                row['Country'] == dag_run.conf['file_data']['country'] and
                                row['Source'] == dag_run.conf['file_data']['parent_company'] and
                                row['URI'] == dag_run.conf['schedule_data']['schedule_name'], config.MAPPER))
        if not payrule_value:
            payrule_value = [
                {} # adding blank dict for below logic to work without failure
                   # (It will fail while doing the service call which is excepted as per the workato logic)
            ]
        return [
            {
                "payRuleScript": {
                    "uri": null,
                    "name":payrule_value[0].get('Value')
                },
                "effectiveDate": null
            }
        ]

    else:
        if dag_run.conf['payrule'].get('payrule'):
            return [
                {
                    "payRuleScript": {
                        "uri": null,
                        "name": dag_run.conf['payrule']['payrule'] #! from input
                    },
                    "effectiveDate": null
                }
            ]

    # default return
    return []

# costCenterSchedule
def _get_cost_center_payload_for_aus_user(dag_run):
    if not dag_run.conf['groups']['cost_center'].get('uri'):
        return []
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

# departmentGroupSchedule
def _get_organizational_unit_payload_for_aus_user(dag_run):
    if not dag_run.conf['groups']['department'].get('uri'):
        return []
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

# locationSchedule
def _get_location_payload_for_aus_user(dag_run):
    if not dag_run.conf['groups']['location'].get('uri'):
        return []
    return [
        {
            "location": {
                "uri": dag_run.conf['groups']['location']['uri'],
                "parent": null,
                "name": null,
                "parameterCorrelationId": null
            },
            "effectiveDate": null
        }
    ]

# divisionSchedule
def _get_company_code_payload_for_aus_user(dag_run):
    if not dag_run.conf['groups']['division'].get('uri'):
        return []
    return [
        {
            "division": {
                "uri": dag_run.conf['groups']['division']['uri'],
                "parent": null,
                "name": null,
                "parameterCorrelationId": null
            },
            "effectiveDate": null
        }
    ]

# serviceCenterSchedule
def _get_pay_group_payload_for_aus_user(dag_run):
    if not dag_run.conf['file_data']['pay_group']:
        return []
    return [
        {
            "serviceCenter": {
                "uri": null,
                "parent": null,
                "name": dag_run.conf['file_data']['pay_group'],
                "parameterCorrelationId": null
            },
            "effectiveDate": null
        }
    ]

# assignedActivities
def _get_activities_to_assign_payload_for_aus_user(dag_run):
    if not dag_run.conf['activities']['activity']:
        return []
    activities_to_assign = []
    for activity in dag_run.conf['activities']['activity'].split("|"):
        activities_to_assign.append({"name": activity})

    return activities_to_assign

def _add_custom_field_for_aus_user(custom_field_uri, text=null, date=null, drop_down_uri=null, drop_down_name=null, number=null):
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

def _get_custom_fields_to_assign_for_aus_user(dag_run):
    custom_fields = dag_run.conf['udfs']
    
    udfs_to_assign = [_add_custom_field_for_aus_user(custom_fields['perner']['uri'], text=dag_run.conf['file_data']['emp_id']),
                      _add_custom_field_for_aus_user(custom_fields['annual_leave_anni_date']['uri'], date=dag_run.conf['json_formatted_dates']['hire_date']),
                      _add_custom_field_for_aus_user(custom_fields['lsl_anniversary_date']['uri'], date=dag_run.conf['json_formatted_dates']['hire_date']),
                      _add_custom_field_for_aus_user(custom_fields['personal_leave_anni_date']['uri'], date=dag_run.conf['json_formatted_dates']['hire_date'])]
    
    if dag_run.conf['file_data']['assignment_type']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['assignment_type']['uri'], text=dag_run.conf['file_data']['assignment_type']))

    if dag_run.conf['file_data']['scheduled_weekly_hours']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['weekly_scheduled_hours']['uri'], text=dag_run.conf['file_data']['scheduled_weekly_hours']))
    
    if dag_run.conf['file_data']['emp_group_name']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['employee_type_udf']['uri'], drop_down_name=f"""{dag_run.conf['file_data']['emp_group_code']}|{dag_run.conf['file_data']['emp_group_name']}"""))

    if dag_run.conf['file_data']['emp_subgroup_name']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['employee_sub_group']['uri'],
                                            drop_down_name=f"{dag_run.conf['file_data']['emp_subgroup_code']}|{dag_run.conf['file_data']['emp_subgroup_name']}"))

    if dag_run.conf['file_data']['perner_id']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['ia_perner_id']['uri'], text=dag_run.conf['file_data']['perner_id']))

    if dag_run.conf['file_data']['terms_conditions']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['terms_and_conditions']['uri'], text=dag_run.conf['file_data']['terms_conditions']))

    if dag_run.conf['file_data']['termination_reason']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['termination_reason']['uri'], text=dag_run.conf['file_data']['termination_reason']))

    # derived from mapper
    if dag_run.conf['mapper_data']['termination_reason_code']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['termination_reason_code']['uri'], text=dag_run.conf['mapper_data']['termination_reason_code']))
    
    # to be added in json_formatted_dates
    if dag_run.conf['file_data']['dob']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['date_of_birth']['uri'], date=dag_run.conf['json_formatted_dates']['date_of_birth']))
    
    if dag_run.conf['file_data']['rut']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['rut']['uri'], text=dag_run.conf['file_data']['rut']))
    
    if dag_run.conf['file_data']['time_type']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['time_type']['uri'], drop_down_name=dag_run.conf['file_data']['time_type']))

    if dag_run.conf['file_data']['gender']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['gender']['uri'], text=dag_run.conf['file_data']['gender']))
    
    if dag_run.conf['file_data']['on_leave']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['on_leave']['uri'], text=dag_run.conf['file_data']['on_leave']))
        
    if dag_run.conf['file_data']['area_code']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['personnel_area_code']['uri'], text=dag_run.conf['file_data']['area_code']))
    
    if dag_run.conf['file_data']['area_name']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['personnel_area_name']['uri'], text=dag_run.conf['file_data']['area_name']))
    
    if dag_run.conf['file_data']['job_level']:
        job_level_prefix = "H" if dag_run.conf['file_data']['country']=="Canada" else ""
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['job_level']['uri'], text=f"{job_level_prefix}{dag_run.conf['file_data']['job_level']}"))
    
    if dag_run.conf['file_data']['fte']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['fte']['uri'], text=dag_run.conf['file_data']['fte']))
        
    if dag_run.conf['file_data']['fte_pct']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['ftepct']['uri'], text=dag_run.conf['file_data']['fte_pct']))
    
    if dag_run.conf['file_data']['is_ia']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['international_assignee']['uri'], text=dag_run.conf['file_data']['is_ia']))
    
    if dag_run.conf['file_data']['service_date']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['service_date']['uri'], date=get_replicon_date(dag_run.conf['file_data']['service_date'])))
    
    if dag_run.conf['file_data']['ia_start_date']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['international_assignee_start_date']['uri'], date=get_replicon_date(dag_run.conf['file_data']['ia_start_date'])))
    
    if not dag_run.conf['file_data']['ia_start_date'] and dag_run.conf['file_data']['is_ia'] in [1, '1']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['international_assignee_start_date']['uri'], date=get_todays_date_in_json()))
    
    if dag_run.conf['file_data']['ia_end_date']:
        udfs_to_assign.append(_add_custom_field_for_aus_user(custom_fields['international_assignee_end_date']['uri'], date=get_replicon_date(dag_run.conf['file_data']['ia_end_date'])))
    
    return udfs_to_assign

def _get_is_login_enabled(dag_run):
    if  (not dag_run.conf['allowed_country']) or (dag_run.conf['allowed_country'] != 'Enable')\
        or (not dag_run.conf['file_data']['parent_company']) or (not dag_run.conf['mapper_data']['profile_status'])\
        or (dag_run.conf['mapper_data']['profile_status'] != 'enabled') or (dag_run.conf['file_data']['on_leave'] in [1, '1']):
            return False
    return True

def _get_timesheet_period_schedule(dag_run):
    if not dag_run.conf['mapper_data']['timesheet_period']:
        return []
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

# /services/ImportService1.svc/PutUser3
def get_create_user_payload(dag_run, config):
    exception_log = []
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['file_data']['email_id'],
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf['file_data']['first_name'],
            "lastname": dag_run.conf['file_data']['last_name'],
            # Only production has the email assignment
            "emailAddress": dag_run.conf['file_data']['email_id'] if config.company_key.lower() == "dxctechnology" else null,
            "employeeId": dag_run.conf['file_data']['emp_id'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": _get_shift_to_assign_payload_for_aus_user(dag_run, exception_log),
            "workWeekStartDayUri": _get_work_week_payload_for_aus_user(dag_run),
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
            "holidayCalendar": _get_holiday_calendar_payload_for_aus_user(dag_run),
            "timeOffPolicy": null,
            "permissionSets": _get_end_user_permission_to_assign_payload_for_aus_user(dag_run),
            "policySets": _get_policies_to_assign_payload_for_aus_user(dag_run),
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": _get_timesheet_approval_path_payload_for_aus_user(dag_run),
            "expenseApprovalPath": null,
            "timeOffApprovalPath": _get_timeoff_approval_path_payload_for_aus_user(dag_run),
            "customFieldValues": _get_custom_fields_to_assign_for_aus_user(dag_run),
            "assignedActivities": _get_activities_to_assign_payload_for_aus_user(dag_run),
            "timeZone": _get_timezone_to_apply(dag_run, exception_log),
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": _get_location_payload_for_aus_user(dag_run),
            "divisionSchedule": _get_company_code_payload_for_aus_user(dag_run),
            "costCenterSchedule": _get_cost_center_payload_for_aus_user(dag_run),
            "serviceCenterSchedule": _get_pay_group_payload_for_aus_user(dag_run),
            "departmentGroupSchedule": _get_organizational_unit_payload_for_aus_user(dag_run),
            "employeeTypeGroupSchedule": _get_employee_type_payload_for_aus_user(dag_run),
            "timesheetPeriodSchedule": _get_timesheet_period_schedule(dag_run),
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [
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
            ],
            "payRuleScriptSchedule": _get_payrule_script_schedule_payload_for_aus_user(dag_run, config),
            "displayNameParameter": {
                "displayName": f"""{dag_run.conf['file_data']['last_name']}, {dag_run.conf['file_data']['first_name']} {dag_run.conf['file_data']['emp_id']} {dag_run.conf['file_data']['email_id']}"""
            }
        }
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

def update_txt_udf(dag_run, input_field_name, udf_display_text_value, udf_key_name, custom_fields_payload, current_custom_fields_values):
    # this field is derived by mapper
    if input_field_name == 'termination_reason_code':
        input_data = dag_run.conf['mapper_data']['termination_reason_code']
    elif isinstance(input_field_name, list):
        input_data = rail.smartjoin_by_delim([dag_run.conf['file_data'][field_name] for field_name in input_field_name], separator='|')
    else:
        input_data = dag_run.conf['file_data'][input_field_name]
    if input_data:
        if input_data != rail.find_first_by_attr_and_get_attr(
            current_custom_fields_values, "customField.displayText", udf_display_text_value, 'text', default=""):
                    custom_fields_payload.append(
                        get_custom_fields_payload(uri=dag_run.conf['udfs'][udf_key_name].get('uri'), txt_value=input_data))
                    if input_field_name == 'on_leave':
                        return True
                    if input_field_name == 'area_code':
                        current_custom_fields_value = rail.find_first_by_attr_and_get_attr(
                                current_custom_fields_values, "customField.displayText", udf_display_text_value, 'text', default="")
                        if input_data == 'AU36' and current_custom_fields_value != 'AU36':
                            # (update_permission_connect, update_permission_general)
                            return True, False
                        if input_data != 'AU36' and current_custom_fields_value == 'AU36':
                            # (update_permission_connect, update_permission_general)
                            return False, True
                        return False, False
                    if input_field_name == 'fte_pct':
                        return True
    if input_field_name == "on_leave":
        return False
    if input_field_name == "area_code":
        # (update_permission_connect, update_permission_general)
        return False, False
    if input_field_name == "fte_pct":
        return False


def update_date_udf(dag_run, input_field_name, json_formatted_date_field_name, udf_display_text_value, udf_key_name, custom_fields_payload, current_custom_fields_values):
    if dag_run.conf['file_data'][input_field_name]:
        if compare_if_two_json_dates_are_same(dag_run.conf['json_formatted_dates'][json_formatted_date_field_name],
                                        rail.find_first_by_attr_and_get_attr(current_custom_fields_values,
                                                                            "customField.displayText", udf_display_text_value, 'date', default="")):
            custom_fields_payload.append(
                get_custom_fields_payload(uri=dag_run.conf['udfs'][udf_key_name].get('uri'), date_value=dag_run.conf['json_formatted_dates'][json_formatted_date_field_name]))

def update_drop_down_udf(dag_run, input_field_name, udf_display_text_value, udf_key_name, custom_fields_payload, current_custom_fields_values):
    if input_field_name == 'termination_reason_code':
        input_data = dag_run.conf['mapper_data']['termination_reason_code']
    elif isinstance(input_field_name, list):
        input_data = rail.smartjoin_by_delim([dag_run.conf['file_data'][field_name] for field_name in input_field_name], separator='|')
    else:
        input_data = dag_run.conf['file_data'][input_field_name]
    if input_data:
        if input_data != rail.find_first_by_attr_and_get_attr(
            current_custom_fields_values, "customField.displayText", udf_display_text_value, 'text', default=""):
            custom_fields_payload.append(
                    get_custom_fields_payload(uri=dag_run.conf['udfs'][udf_key_name].get('uri'), drop_down_value_name=input_data))


def get_custom_fields_to_update_callable(dag_run):
    current_custom_fields_values = rail.result("get_user_details")['userDetails']['customFieldValues']
    custom_fields_payload = []
    update_txt_udf(dag_run, 'assignment_type', 'assignment_type', 'assignment_type', custom_fields_payload, current_custom_fields_values)
    update_drop_down_udf(dag_run, ['emp_group_code', 'emp_group_name'], 'Employee Group', 'employee_type_udf', custom_fields_payload, current_custom_fields_values)
    update_drop_down_udf(dag_run, ['emp_subgroup_code', 'emp_subgroup_name'], 'Employee Sub Group', 'employee_sub_group', custom_fields_payload, current_custom_fields_values)
    update_date_udf(dag_run, 'dob', 'date_of_birth', 'Date of Birth', 'date_of_birth', custom_fields_payload, current_custom_fields_values)
    # Scheudule Weekly hours UDF update only if user is enabled
    if dag_run.conf['replicon_field'] == 'true':
        update_txt_udf(dag_run, 'scheduled_weekly_hours', 'Weekly Scheduled Hours', 'weekly_scheduled_hours', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'terms_conditions', 'Terms and Conditions', 'terms_and_conditions', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'termination_reason', 'Termination Reason', 'termination_reason', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'termination_reason_code', 'Termination Reason Code', 'termination_reason_code', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'middle_name', 'Middle Name', 'middle_name', custom_fields_payload, current_custom_fields_values)
    update_drop_down_udf(dag_run, 'time_type', 'Time Type', 'time_type', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'perner_id', 'IA PERNER ID', 'ia_perner_id', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'gender', 'Gender', 'gender', custom_fields_payload, current_custom_fields_values)
    on_leave_status_update = update_txt_udf(dag_run, 'on_leave', 'On Leave', 'on_leave', custom_fields_payload, current_custom_fields_values)
    update_permission_connect, update_permission_general = update_txt_udf(dag_run, 'area_code', 'Personnel Area Code', 'personnel_area_code', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'area_name', 'Personnel Area Description', 'personnel_area_name', custom_fields_payload, current_custom_fields_values)
    # For canada has some custom logic but in GSAP setup will not have canada country
    update_txt_udf(dag_run, 'job_level', 'Job Activity Type', 'job_level', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'fte', 'FTE', 'fte', custom_fields_payload, current_custom_fields_values)
    fte_updated = update_txt_udf(dag_run, 'fte_pct', 'FTE %', 'ftepct', custom_fields_payload, current_custom_fields_values)

    # For australia the effective date is being used in later update
    # UDF updates 1st then user other details gets updated.
    ia_updated, ia_exception_msg, _ = get_ia_update_payload_for_udf_update(dag_run, custom_fields_payload, current_custom_fields_values, update_date_udf=update_date_udf, update_txt_udf=update_txt_udf)   

    update_date_udf(dag_run, 'service_date', 'service_date', 'Continuous Service Date', 'service_date', custom_fields_payload, current_custom_fields_values)
    rail.set_result(key="ia_updated", val=False)
    rail.set_result(key="ia_exception_msg", val="")
    rail.set_result(key="on_leave_status_update", val=on_leave_status_update)
    rail.set_result(key="fte_updated", val=fte_updated)
    rail.set_result(key="permission_change", val={
        "update_permission_general": update_permission_general,
        "update_permission_connect": update_permission_connect
        }
    )

    return custom_fields_payload


def can_update_first_name(dag_run, user_details):
    if dag_run.conf['file_data']['first_name'] != user_details['userDetails']['firstName']:
        return True
    return False

def can_update_last_name(dag_run, user_details):
    if dag_run.conf['file_data']['last_name'] != user_details['userDetails']['lastName']:
        return True
    return False

def can_update_start_date(dag_run, user_details):
    if dag_run.conf['json_formatted_dates']['hire_date'] and dag_run.conf['json_formatted_dates']['hire_date'] != user_details['userDetails']['employmentDateRange']['startDate']:
        return True
    return False

def can_update_email(dag_run, user_details, config):
    if config.instance == "prod" or config.instance == "trial":
        if dag_run.conf['file_data']['email_id']:
            if user_details['securityConfiguration']['loginName'] !=  dag_run.conf['file_data']['email_id']:
                # email_update, security_config_update
                return True, True
            if not user_details['userDetails']['emailAddress']:
                return True, False
    return False, False

def can_update_display_text(dag_run, user_details, config):
    email_update, _ = can_update_email(dag_run, user_details, config)
    return can_update_first_name(dag_run, user_details) or can_update_last_name(dag_run, user_details) or email_update

def get_two_date_diff(effective_date, user_start_date, today):
    if effective_date:
        return convert_json_date_to_date(today) - convert_json_date_to_date(effective_date)
    return convert_json_date_to_date(today) -  convert_json_date_to_date(user_start_date)

def get_current_payrule_schedule_timesheetPeriod(payrule_schedule_details, user_start_date):
    current_effective_payrule = None
    #! can be optimized
    current_min_day_diff = "*"
    today= get_todays_date_in_json()
    for _schedule in payrule_schedule_details:
        day_diff_cnt = get_two_date_diff(_schedule['effectiveDate'], user_start_date, today)
        
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

def get_effective_date_for_payrule_gsap():
    today =datetime.now()
    if today.weekday() == 6:
        return today + timedelta(days=6)
    return today+timedelta(days=(5-today.weekday()))

days_mapper = {
    6:7,
}

def get_effective_date_for_payrule_by_work_week(work_week:str):
    is_work_week_starts_with_saturday = work_week.lower().split(" ")[0] == 'saturday'
    is_work_week_starts_with_sunday = work_week.lower().split(" ")[0] == 'sunday'

    today = datetime.now()

    if is_work_week_starts_with_saturday:
        if today.weekday() == 5:
            return today
        if today.weekday() == 6:
            return today - timedelta(days=1)
        return today - timedelta(days=today.weekday()+2)
    if is_work_week_starts_with_sunday:
        if today.weekday() == 6:
            return today
        return today - timedelta(days=today.weekday()+1)
    return today - timedelta(days=days_mapper.get(today.weekday(), today.weekday()))

 
def get_effective_date_for_payrule(dag_run):
    if dag_run.conf['file_data']['parent_company'].lower() == 'gsap':
        return get_effective_date_for_payrule_gsap()
    effective_date_for_payrule = get_effective_date_for_payrule_by_work_week(dag_run.conf['mapper_data']['work_week'])
    if effective_date_for_payrule == (effective_date_for_payrule).replace(day=1):
        return effective_date_for_payrule
    return (effective_date_for_payrule + relativedelta(months=1)).replace(day=1)


def get_payrule_to_apply(dag_run, user_details, config, effective_date_to_use):
    country = dag_run.conf['file_data']['country']
    current_payrule_schedule = get_current_payrule_schedule_timesheetPeriod(
        user_details['payRuleScriptSchedule'], user_details['userDetails']['employmentDateRange']['startDate'])
    mapper_value = list(filter(lambda row: row['Type']=='Payrule' and\
                    row['Function']=='Workday User Sync' and\
                    row['Country'] == country and\
                    row['Source'] == dag_run.conf['file_data']['parent_company'] and\
                    row['URI'] == dag_run.conf['schedule_data']['schedule_name'] ,config.MAPPER))
    if not mapper_value:
        """
            Ideally there should not be any scenario where payrule or any other attributes is blank from mapper
            to avoid failure here added below, although it will fail while doing service call
            Kept this way as rather than failing here which would through KeyError, a service call failure would provide more info
            for easy debugging of the error
            Note: In workato the behavior is kinda same ## recipe:46133936 | step:164
        """
        mapper_value = [{}]
    payrule_effective_date = get_effective_date_for_payrule(dag_run)
    if dag_run.conf['file_data']['industrial_instrument_classification'] and\
        dag_run.conf['schedule_data']['schedule_name'] == 'Shift' and "BFI" in dag_run.conf['file_data']['industrial_instrument_classification']:
        if (not current_payrule_schedule) or (mapper_value[0].get('Value', '') != current_payrule_schedule['payRuleScript']['displayText']):
            return {
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri" : null,
                            "name": dag_run.conf['payrule']['payrule']
                        },
                        "effectiveDate": effective_date_to_use if effective_date_to_use else {
                            "day": payrule_effective_date.day,
                            "month": payrule_effective_date.month,
                            "year": payrule_effective_date.year
                        }
                    }
                ]
            }
    else:
        if (dag_run.conf['payrule']['payrule']) and (not current_payrule_schedule or current_payrule_schedule['payRuleScript']['displayText'] != dag_run.conf['payrule']['payrule']):
            return {
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri" : null,
                            "name": dag_run.conf['payrule']['payrule']
                        },
                        "effectiveDate": {
                            "day": payrule_effective_date.day,
                            "month": payrule_effective_date.month,
                            "year": payrule_effective_date.year
                        }
                    }
                ]
            }

    return null


def get_schedule_to_update(dag_run, user_details, exception_log):
    current_office_schedule = get_current_payrule_schedule_timesheetPeriod(
        user_details['schedulePolicies'], user_details['userDetails']['employmentDateRange']['startDate'])
    if dag_run.conf['schedule_data']['schedule_name'] == 'Shift':
        if not current_office_schedule or\
                  (current_office_schedule['scheduleTypeUri'] != dag_run.conf['schedule_data']['schedule_type_uri']):
            if dag_run.conf['file_data']['parent_company'].lower() == 'compass':
                if compare_two_dates(convert_json_date_to_date(dag_run.conf['json_formatted_dates']['job_change_effective_date']), 
                                            convert_json_date_to_date(dag_run.conf['json_formatted_dates']['aus_job_change_effective_date_sample_date']), '<'):
                    # under above if condition 
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
                                                "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri']
                                            },
                                            "effectiveDate": dag_run.conf['json_formatted_dates']['aus_job_change_effective_date_sample_date']
                                        }
                                    ],
                                    "endDate": null
                                }
                            }
                else:
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
                                                "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri']
                                            },
                                            "effectiveDate": dag_run.conf['json_formatted_dates']['job_change_effective_date']
                                        }
                                    ],
                                    "endDate": null
                                }
                            }
            else:
                # else of parent_company is compass
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
                                        "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri']
                                    },
                                    "effectiveDate": dag_run.conf['json_formatted_dates']['work_shift_effective_date']
                                }
                            ],
                            "endDate": null
                        }
                    } 
    else:
        if dag_run.conf['schedule_data']['schedule_name']:
            office_schedule_uri = current_office_schedule['officeSchedule']['uri'] if current_office_schedule['officeSchedule'] else ''
            if (not current_office_schedule) or (not current_office_schedule['officeSchedule']) or\
                  (office_schedule_uri != dag_run.conf['schedule_data']['schedule_uri']):
                if dag_run.conf['schedule_data']['schedule_uri']:
                    if dag_run.conf['file_data']['parent_company'].lower() == 'compass':
                        if compare_two_dates(convert_json_date_to_date(dag_run.conf['json_formatted_dates']['job_change_effective_date']), 
                                            convert_json_date_to_date(dag_run.conf['json_formatted_dates']['aus_job_change_effective_date_sample_date']), '<'):
                            return {
                                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                                "replacementSchedule": [],
                                "updateScheduleOverDateRange": {
                                    "replacementScheduleEntries": [
                                        {
                                            "schedulePolicy": {
                                                "officeScheduleUri": dag_run.conf['schedule_data']['schedule_uri'],
                                                "name": null,
                                                "officeSchedule": {
                                                    "officeScheduleUri": dag_run.conf['schedule_data']['schedule_uri'],
                                                    "name": null
                                                },
                                                "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri']
                                            },
                                            "effectiveDate": dag_run.conf['json_formatted_dates']['aus_job_change_effective_date_sample_date']
                                        }
                                    ],
                                    "endDate": null
                                }
                            }
                        return {
                                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                                "replacementSchedule": [],
                                "updateScheduleOverDateRange": {
                                    "replacementScheduleEntries": [
                                        {
                                            "schedulePolicy": {
                                                "officeScheduleUri": dag_run.conf['schedule_data']['schedule_uri'],
                                                "name": null,
                                                "officeSchedule": {
                                                    "officeScheduleUri": dag_run.conf['schedule_data']['schedule_uri'],
                                                    "name": null
                                                },
                                                "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri']
                                            },
                                            "effectiveDate": dag_run.conf['json_formatted_dates']['job_change_effective_date']
                                        }
                                    ],
                                    "endDate": null
                                }
                            }
                    return {
                        "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementSchedule": [],
                        "updateScheduleOverDateRange": {
                            "replacementScheduleEntries": [
                                {
                                    "schedulePolicy": {
                                        "officeScheduleUri": null,
                                        "name": dag_run.conf['schedule_data']['work_schedule'],
                                        "officeSchedule": {
                                            "officeScheduleUri": null,
                                            "name": dag_run.conf['schedule_data']['work_schedule']
                                        },
                                        "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri']
                                    },
                                    "effectiveDate": dag_run.conf['json_formatted_dates']['work_shift_effective_date']
                                }
                            ],
                            "endDate": null
                        }
                    }
                else:
                    # else of schedule uri is not present
                    exception_log.append(f"""Office schedule "{dag_run.conf['file_data']['work_shift']}" not available in Replicon""")
    return null


def can_update_time_entry_approval_path(dag_run):
    if not rail.result("get_time_entry_approval_path")['displayText']:
        return True
    if (dag_run.conf['mapper_data']['time_entry_approval_path_name']) and (rail.result("get_timeentry_approval_path_for_user")['displayText'] != dag_run.conf['mapper_data']['time_entry_approval_path_name']):
        return True
    return False

def can_update_punch_entry_policies(dag_run):
    # for GSAP the Punchentrypolicy is always null
    if not  dag_run.conf['policy_sets'].get('punch_entry_policy', {}).get('name', ''):
        return False
    current_assigned_policies = rail.result("get_assigned_policy_sets_for_user")
    return not bool(list(filter(lambda time_punch_policy: time_punch_policy['policySet']['name'] == dag_run.conf['policy_sets']['punch_entry_policy'].get('name', ''), 
                filter(lambda policy: policy['policyUri']=="urn:replicon:policy:time-punch", current_assigned_policies))))


def get_timesheet_period_to_apply(dag_run, user_details, work_week_effective_date, effective_date_to_use):
    if dag_run.conf['mapper_data']['timesheet_period']:
        current_timesheet_period = get_current_payrule_schedule_timesheetPeriod(user_details['timesheetPeriodSchedule'], 
                                                                            user_details['userDetails']['employmentDateRange']['startDate'])
        if current_timesheet_period:
            if current_timesheet_period['timesheetPeriod']['displayText'] != dag_run.conf['mapper_data']['timesheet_period']:
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
                                "effectiveDate": {
                                    "year": work_week_effective_date.year,
                                    "month": work_week_effective_date.month,
                                    "day": work_week_effective_date.day
                                }
                            }
                        ],
                        "endDate": null
                    }
                }
        
    return null


def get_department_update_payload(dag_run, current_effective_grps, work_week_effective_date, effective_date_to_use):
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
                        "effectiveDate": effective_date_to_use if effective_date_to_use else {
                                    "year": work_week_effective_date.year,
                                    "month": work_week_effective_date.month,
                                    "day": work_week_effective_date.day
                                }
                    }
                ],
                "endDate": null
            }
        }
    return null

def get_location_update_payload(dag_run, current_effective_grps, effective_date_to_use):
    
    if dag_run.conf['groups']['location'] and dag_run.conf['groups']['location'].get('uri' ,'') != current_effective_grps['location'].get('uri' ,''):
        rail.set_result(key="location_updated",val= "yes")
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
                        "effectiveDate": effective_date_to_use if effective_date_to_use else dag_run.conf['json_formatted_dates']['location_effective_date']
                    }
                ],
                "endDate": null
            }
        }
    
    return null

def get_cost_center_update_payload(dag_run, current_effective_grps, effective_date_to_use):
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
                        "effectiveDate": effective_date_to_use if effective_date_to_use else dag_run.conf['json_formatted_dates']['cost_center_effective_date']
                    }
                ],
                "endDate": null
            }
        }
    return null

def get_employee_type_update_payload(dag_run, current_effective_grps, effective_date_to_use):
    if dag_run.conf['groups']['employee_type']['uri'] and dag_run.conf['groups']['employee_type']['uri'] != current_effective_grps['employeeType'].get('uri', ''):
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
                        "effectiveDate": effective_date_to_use if effective_date_to_use else dag_run.conf['json_formatted_dates']['employee_type_effective_date']
                    }
                ],
                "endDate": null
            }
        }
    return null

def get_service_center_update_payload(dag_run, current_effective_grps, work_week_effective_date, effective_date_to_use):
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
                    "effectiveDate": effective_date_to_use if effective_date_to_use else {
                        "year": work_week_effective_date.year,
                        "month": work_week_effective_date.month,
                        "day": work_week_effective_date.day
                    }
                }
                ],
                "endDate": null
            }
        }
    
    return null

def get_division_update_payload(dag_run, current_effective_grps, effective_date_to_use):

    if dag_run.conf['groups']['division'].get('uri' ,'') and dag_run.conf['groups']['division'].get('uri' ,'') != current_effective_grps['division'].get('uri' ,''):
        
        update_connect_permission, update_general_permission = False, False
        if current_effective_grps['division'].get('name', '') == '3124' and dag_run.conf['groups']['division'].get('name' ,'') != '3124':
            update_connect_permission = True
        if current_effective_grps['division'].get('name', '') != '3124' and dag_run.conf['groups']['division'].get('name' ,'') == '3124':
            update_general_permission = True
        rail.set_result(key="permission_update", val={"update_general_permission": update_general_permission,
                                                      "update_connect_permission": update_connect_permission})
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
                            "effectiveDate": effective_date_to_use if effective_date_to_use else (dag_run.conf['json_formatted_dates']['job_change_effective_date'] if dag_run.conf[
                                'file_data']['job_change_effective_date'] else dag_run.conf['json_formatted_dates']['cost_center_effective_date'])
                        }
                    ],
                    "endDate": null
                }
            }
    return null

def get_timezone_to_update(dag_run,user_details, exception_log):
    if dag_run.conf['timezone']['timezone']:
        if dag_run.conf['timezone']['timezone_uri'] != user_details['timeZone']['uri']:
            return  {
                "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": {
                "uri": dag_run.conf['timezone']['timezone_uri'],
                "IANAName": null
                }
            }
        return null
    exception_log.append(f"Timezone not defined in mapper for Location {dag_run.conf['file_data']['country']}")
    return null

def get_activities_to_update(dag_run, user_details):
    if dag_run.conf['activities']['activity']:
        activity_list = dag_run.conf['activities']['activity'].split('|')
        user_activities = [user_assigned_activity['name'] for user_assigned_activity in user_details['assignedActivities']]
        can_assign_activities = False
        for activity in activity_list:
            if activity not in user_activities:
                can_assign_activities = True
                break
        if can_assign_activities:
            return list(map(lambda _activity: {
                    "uri": null,
                    "name": _activity,
                }, activity_list))
    # in the payload the activity is set to []
    # if we return [] it will remove all the assigned activities
    # to avoid this we return null which doesn't remove activities
    return null

def get_timesheet_period_to_apply_profile_status_enabled(dag_run, user_details):
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

def get_timesheetperiod_update_payload(dag_run, user_details, effective_date_to_use):
    work_week_effective_date = get_work_week_based_effective_date(dag_run.conf['mapper_data']['work_week'])
    if dag_run.conf['mapper_data']['profile_status'].lower() != "enabled":
        return get_timesheet_period_to_apply(dag_run, user_details, work_week_effective_date, effective_date_to_use)
    if dag_run.conf['mapper_data']['profile_status'].lower() == "enabled":
        return get_timesheet_period_to_apply_profile_status_enabled(dag_run, user_details)
    # this will never get executed as per above conditions are if-else
    return null

def get_work_week_update_payload(dag_run):
    if dag_run.conf['mapper_data']['work_week_uri']:
        return {
            "workWeekStartDayUri": dag_run.conf['mapper_data']['work_week_uri']
        }
    return null

def get_timeoff_approval_update_payload(dag_run):
    if dag_run.conf['mapper_data']['timeoff_approval']:
        return {
            "uri": null,
            "name": dag_run.conf['mapper_data']['timeoff_approval']
        }
    return null

def can_update_timesheet_template(dag_run, user_details):
    if dag_run.conf['mapper_data']['timesheet_template']:
        timesheet_template = user_details['timesheetTemplate']['name'] if user_details['timesheetTemplate'] else ''
        if (not timesheet_template) or (timesheet_template != dag_run.conf['mapper_data']['timesheet_template']):
            return True
    return False

def can_update_timeoff_template(dag_run, user_details):
    if dag_run.conf['mapper_data']['timeoff_template']:
        timeoff_template = user_details['timeOffTemplate'].get('name', '') if user_details['timeOffTemplate'] else ''
        if (not timeoff_template) or (timeoff_template != dag_run.conf['mapper_data']['timeoff_template']):
            return True
    return False

def get_policies_to_update_payload(dag_run, user_details, exception_log):
    policies_to_add = []
    if can_update_punch_entry_policies(dag_run):
        policies_to_add.append(dag_run.conf['policy_sets']['punch_entry_policy']['uri'])

    if can_update_timesheet_template(dag_run, user_details):
        if dag_run.conf['policy_sets']['timesheet_template'].get('uri'):
            policies_to_add.append(dag_run.conf['policy_sets']['timesheet_template']['uri'])
        else:
            exception_log.append(f"Timesheet template {dag_run.conf['mapper_data']['timesheet_template']} not available in Replicon")

    if can_update_timeoff_template(dag_run, user_details):
        if dag_run.conf['policy_sets']['timeoff_template'].get('uri'):
            policies_to_add.append(dag_run.conf['policy_sets']['timeoff_template']['uri'])
        else:
            exception_log.append(f"Timesheet template {dag_run.conf['mapper_data']['timesheet_template']} not available in Replicon")

    return {
                "policySetUrisToAssign": policies_to_add,
                "policyUrisToRemovePolicySet": [],
                "policySetUrisToRemove": []
            } if policies_to_add else null

def can_update_holiday_calendar(dag_run, user_details, exception_log):
    if dag_run.conf['mapper_data']['holiday_calendar']:
        holiday_calendar =  user_details['holidayCalendar'].get('displayText', '') if user_details['holidayCalendar'] else ''
        if dag_run.conf['mapper_data']['holiday_calendar'] != holiday_calendar:
            if dag_run.conf['mapper_data']['holiday_calendar_uri']:
                return True
            exception_log.append(f''''Holiday calendar "{dag_run.conf['mapper_data']['holiday_calendar']}" not available in Replicon''')
    return False

def get_holiday_calendar_update_payload(dag_run, user_details, exception_log):
    if can_update_holiday_calendar(dag_run, user_details, exception_log):
        return {
        "holidayCalendar": {
            "uri": dag_run.conf['mapper_data']['holiday_calendar_uri']['uri'],
            "name": null
        }
      }
    return null

def update_user_payload(dag_run, config):

    user_details = rail.result("get_user_details")
    effective_date_to_use = null

    email_update, security_config_update = can_update_email(dag_run, user_details, config)

    current_effective_grps = rail.result("get_effective_group_membership")
    exception_log = []
    work_week_effective_date = get_effective_date_for_payrule_by_work_week(dag_run.conf['mapper_data']['work_week'])
    payload = {
        "user": {
            "uri": dag_run.conf['user_uri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "timezoneToApply": get_timezone_to_update(dag_run, user_details,exception_log),
            "workWeekStartToApply": get_work_week_update_payload(dag_run),
            "holidayCalendarToApply": get_holiday_calendar_update_payload(dag_run, user_details, exception_log),
            "holidayCalendarAssignmentsToApply": null,
            "schedulePolicyToApply": get_schedule_to_update(dag_run, user_details, exception_log),
            "locationScheduleToApply": get_location_update_payload(dag_run, current_effective_grps, effective_date_to_use),
            "divisionScheduleToApply": get_division_update_payload(dag_run, current_effective_grps, effective_date_to_use),
            "costCenterScheduleToApply": get_cost_center_update_payload(dag_run, current_effective_grps, effective_date_to_use),
            "departmentGroupScheduleToApply": get_department_update_payload(dag_run,current_effective_grps, work_week_effective_date, effective_date_to_use),
            "employeeTypeGroupScheduleToApply": get_employee_type_update_payload(dag_run, current_effective_grps, effective_date_to_use),
            "timesheetPeriodScheduleToApply": get_timesheetperiod_update_payload(dag_run, user_details, effective_date_to_use),
            "serviceCenterScheduleToApply": get_service_center_update_payload(dag_run, current_effective_grps, work_week_effective_date, effective_date_to_use),
            "totalBusinessCostScheduleToApply": null,
            "permissionSetsToApply": null,
            "policySetsToApply": get_policies_to_update_payload(dag_run, user_details, exception_log),
            "policyDataAccessScopesToApply": null,
            "policyDataAccessScopesToApply2": null,
            "notificationPreferencesToApply": null,
            "timesheetPeriodTypeToApply": null,
            "timesheetApprovalPathToApply": null,
            "timeEntryRevisionGroupApprovalPathToApply":{
                "uri": null,
                "name": dag_run.conf['mapper_data']['time_entry_approval_path_name']
            } if can_update_time_entry_approval_path(dag_run) else null, #206
            "validationRuleToApply": null,
            "activitiesToApply": get_activities_to_update(dag_run, user_details),
            "activitiesToApply2": null,
            "defaultActivityToApply": null,
            "defaultActivityToApply2": null,
            "defaultTimeOffTypeForBookingsToApply": null,
            "expenseApprovalPathToApply": null,
            "expenseDefaultReimbursementCurrencyToApply": null,
            "timeOffApprovalPathToApply": get_timeoff_approval_update_payload(dag_run),
            "productAssignmentsToApply": null,
            "timeBankPolicyToApply": null,
            "securitySettingsToApply": {
                    "loginEnabled": "true",
                    "forcePasswordChange": "false",
                    "loginName": dag_run.conf['file_data']['email_id'],
                    "sso": dag_run.conf['file_data']['email_id'],
                    "password": None,
                    "enabledAuthenticationTypeUris": ["urn:replicon:user-authentication-type:sso"],
                    "userSSONameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name"
            } if security_config_update else null,
            "supervisorsToApply": null,
            "supervisorsModifications": null,
            "payrollRatesToApply": null,
            "payrollRatesModifications": null,
            "overtimeRulesToApply": null,
            "overtimeRulesModifications": null,
            # Custom felids are getting updated at the start as per workato 
            "customFieldValuesToApply": [],
            "departmentToApply": null,
            "employeeTypeToApply": null,
            "userDetailsToApply": {
                "firstName": dag_run.conf['file_data']['first_name'] if can_update_first_name(dag_run, user_details) else null,
                "lastName": dag_run.conf['file_data']['last_name'] if can_update_last_name(dag_run, user_details) else null,
                "emailAddress": {
                    "emailAddress": dag_run.conf['file_data']['email_id']
                } if email_update else null,
                "language": null,
                "employmentDateRange": {
                    "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                    # this update is done at the start rather than creating a task, added here
                } if can_update_start_date(dag_run, user_details) else null,
                "employmentStartDate": null,
                "employmentEndDate": null,
                "employeeId": null,
                "displayNameParameter": {
                    "displayName": f"""{dag_run.conf['file_data']['last_name']}, {dag_run.conf['file_data']['first_name']} {dag_run.conf['file_data']['emp_id']} {dag_run.conf['file_data']['email_id']}"""
                } if can_update_display_text(dag_run, user_details, config)  else null
            } if can_update_start_date(dag_run, user_details) or can_update_first_name(dag_run, user_details) or can_update_last_name(dag_run, user_details) else null,
            "payRulesToApply": null,
            "payRulesScheduleModifications": get_payrule_to_apply(dag_run, user_details, config, effective_date_to_use),
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
    rail.set_result(key="exception_log", val=exception_log)
    return payload