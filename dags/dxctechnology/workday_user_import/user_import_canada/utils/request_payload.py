import ast
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from json import dumps, loads
import rail
from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import convert_json_date_to_date, get_ia_update_payload_for_udf_update, compare_if_two_json_dates_are_same
from dxctechnology.workday_user_import.user_import.common_utils.request_payload import get_todays_date_in_json, TIMESHEET_PERIOD_EFFECTIVE_DATE
from dxctechnology.workday_user_import.user_import_canada.utils.custom_methods import is_profile_enabled, get_replicon_date

null = None

# Add User
def _get_schedule_policy_schedule_payload(dag_run, exception_log):
    schedule_name = ""
    if dag_run.conf['file_data']['work_shift']:
        if dag_run.conf['schedule_data']['work_schedule_uri']:
            schedule_name = dag_run.conf['file_data']['work_shift']
        else:
            exception_log.append(f"Office schedule {dag_run.conf['file_data']['work_shift']} not available in Replicon. Hence default shift assigned")
            schedule_name = dag_run.conf['mapper_data']['office_schedule']
    else:
        if dag_run.conf['mapper_data']['office_schedule']:
            schedule_name = dag_run.conf['mapper_data']['office_schedule']

    if schedule_name:
        return [
            {
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": schedule_name,
                    "officeSchedule": {
                        "officeScheduleUri": null,
                        "name": schedule_name
                    },
                    "scheduleTypeUri": dag_run.conf['mapper_data']['schedule_type_uri']
                },
                "effectiveDate": null
            }
        ]
    return []


def _is_user_enabled(dag_run) -> bool:   
    if dag_run.conf['file_data']['on_leave'] in [1, '1']:
        return False

    if not dag_run.conf['allowed_country'] or dag_run.conf['allowed_country'] != "Enable" or \
        not dag_run.conf['file_data']['parent_company'] or not dag_run.conf['mapper_data']['profile_status'] or \
            dag_run.conf['mapper_data']['profile_status'] != "enabled":
        return False

    return True


def _get_holiday_calendar_to_assign(dag_run):
    if not dag_run.conf['mapper_data']["holiday_calendar_uri"]:
        return null
    return {
      "uri": dag_run.conf['mapper_data']["holiday_calendar_uri"]['uri'],
      "name": null
    }


def _get_permission_sets_to_assign(dag_run):
    if dag_run.conf['user_permission_sets']['end_user_permission'].get('uri'):
        return [
            {
                "uri": dag_run.conf['user_permission_sets']['end_user_permission']['uri'],
                "name": null
            }
        ]   
    return []


def _get_policy_sets_to_assign(dag_run):
    policies = []
    if dag_run.conf['policy_sets']['timesheet_template'] and dag_run.conf['mapper_data']['profile_status'] == "enabled":
        if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
            policies.append({"name": dag_run.conf['policy_sets']['timesheet_template']['name']})
    if dag_run.conf['policy_sets']['timeoff_template'] and dag_run.conf['mapper_data']['profile_status'] == "enabled":
        policies.append({"name": dag_run.conf['policy_sets']['timeoff_template']['name']})

    return policies


def _get_timesheet_approval_path_to_assign(dag_run):
    if dag_run.conf['mapper_data']['timesheet_approval_path']:
        return {
            "uri": null,
            "name": dag_run.conf['mapper_data']['timesheet_approval_path']
        }
    return null


def _get_time_off_approval_path_to_assign(dag_run):
    if dag_run.conf['mapper_data']['timeoff_approval']:
        return {
            "uri": null,
            "name": dag_run.conf['mapper_data']['timeoff_approval']
        }
    return null


def _add_custom_field(custom_field_uri, text=null, date=null, drop_down_uri=null, number=null):
    return {
        "customField": {
            "uri" : custom_field_uri
        },
        "text": text,
        "date": date,
        "dropDownOption": {
            "uri": drop_down_uri,
            "name": null
        } if drop_down_uri else null,
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
        udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_start_date']['uri'], date=get_todays_date_in_json()))
    
    if dag_run.conf['file_data']['ia_end_date']:
        udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_end_date']['uri'], date=get_replicon_date(dag_run.conf['file_data']['ia_end_date'])))
    
    return udfs_to_assign


def _get_activities_to_assign(dag_run):
    if not dag_run.conf['activities']['activity']:
        return []
    activities_to_assign = []
    for activity in dag_run.conf['activities']['activity'].split("|"):
        activities_to_assign.append({"name": activity})

    return activities_to_assign


def _get_timezone_to_assign(dag_run, exception_log):
    if dag_run.conf['timezone']['timezone_uri']:
        return {
            "uri": dag_run.conf['timezone']['timezone_uri']
        }
    exception_log.append(f"Time Zone not defined for country {dag_run.conf['file_data']['country']} in mapper")
    return null


def _get_policy_data_access_scope2():
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


def _get_location_schedule_to_assign(dag_run):
    if dag_run.conf['groups']['location'].get('uri'):
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
    return [] 


def _get_division_schedule_to_assign(dag_run):
    if dag_run.conf['groups']['division'].get('uri'):
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
    return []


def _get_cost_center_schedule_to_assign(dag_run):
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
    return []


def _get_service_center_schedule_to_assign(dag_run):
    if dag_run.conf['file_data']['pay_group']:
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
    return []


def _get_employee_type_schedule_to_assign(dag_run):
    if dag_run.conf['groups']['employee_type']['other'].get('uri'):
        return [
            {
                "employeeTypeGroup": {
                    "uri": dag_run.conf['groups']['employee_type']['other']['uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }
        ]
    return []


def _get_department_schedule_to_assign(dag_run):
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
    return []


def _get_timesheet_period_schedule_to_assign(dag_run):
    effective_date = null
    if dag_run.conf['item']['timesheet_period_effective_date'] and \
        get_replicon_date(dag_run.conf['item']['timesheet_period_effective_date'], "date", TIMESHEET_PERIOD_EFFECTIVE_DATE) > get_replicon_date(dag_run.conf['file_data']['hire_date'], "date"):
            effective_date = get_replicon_date(dag_run.conf['item']['timesheet_period_effective_date'], "dict", TIMESHEET_PERIOD_EFFECTIVE_DATE)

    return [
      {
        "timesheetPeriod": {
          "uri": null,
          "name": dag_run.conf['mapper_data']['timesheet_period']
        },
        "effectiveDate": effective_date
      }
    ]


def _get_payrule_script_schedule_to_assign(dag_run):
    if dag_run.conf['payrule']['payrule'] and dag_run.conf['file_data']['country'] == "Canada" and dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
        return [
            {
                "payRuleScript": {
                    "uri": null,
                    "name": dag_run.conf['payrule']['payrule']
                },
                "effectiveDate": null
            }
        ]
    return []


def get_user_creation_payload(dag_run, config):
    exception_log = []
    payload = {}
    payload["user"] = {
        "target":{
            "loginName": dag_run.conf['file_data']['email_id']
        }
    }

    payload["user"]['firstname'] = dag_run.conf['file_data']['first_name']
    payload["user"]['lastname'] = dag_run.conf['file_data']['last_name']
    
    # this will be only applicable for the Production
    if config.company_key.lower() == "dxctechnology":
        payload["user"]['emailAddress'] = dag_run.conf['file_data']['email_id']

    payload["user"]['employeeId'] = dag_run.conf['file_data']['emp_id']
    payload["user"]['schedulePolicySchedule'] = _get_schedule_policy_schedule_payload(dag_run, exception_log)
    payload["user"]['workWeekStartDayUri'] = dag_run.conf['mapper_data']['work_week_uri'] if dag_run.conf['mapper_data']['work_week_uri'] else null
    payload["user"]['employmentDateRange'] = {
        "startDate" : get_replicon_date(dag_run.conf['file_data']['hire_date'])
    }

    payload["user"]['securityConfiguration'] = {
        "enabledAuthenticationTypeUris": [
            dag_run.conf['mapper_data']['authentication_uri']
        ],
        "isLoginEnabled": _is_user_enabled(dag_run),
        "loginName": dag_run.conf['file_data']['email_id'],
        "SSOName": dag_run.conf['file_data']['email_id'],
        "password": null
    }

    payload["user"]['holidayCalendar'] = _get_holiday_calendar_to_assign(dag_run)
    payload["user"]['permissionSets'] = _get_permission_sets_to_assign(dag_run)
    payload["user"]['policySets'] = _get_policy_sets_to_assign(dag_run)
    payload["user"]['timesheetApprovalPath'] = _get_timesheet_approval_path_to_assign(dag_run)
    payload["user"]['timeOffApprovalPath'] = _get_time_off_approval_path_to_assign(dag_run)
    payload["user"]['customFieldValues'] = _get_custom_fields_to_assign(dag_run) #! 96 #dag_run update done
    payload["user"]['assignedActivities'] = _get_activities_to_assign(dag_run)
    payload["user"]['timeZone'] = _get_timezone_to_assign(dag_run, exception_log)
    payload["user"]['locationSchedule'] = _get_location_schedule_to_assign(dag_run)
    payload["user"]['divisionSchedule'] = _get_division_schedule_to_assign(dag_run)
    payload["user"]['costCenterSchedule'] = _get_cost_center_schedule_to_assign(dag_run)
    payload["user"]['serviceCenterSchedule'] = _get_service_center_schedule_to_assign(dag_run)
    payload["user"]['departmentGroupSchedule'] = _get_department_schedule_to_assign(dag_run)
    payload["user"]['employeeTypeGroupSchedule'] = _get_employee_type_schedule_to_assign(dag_run)
    payload["user"]['timesheetPeriodSchedule'] = _get_timesheet_period_schedule_to_assign(dag_run)
    payload["user"]['policyDataAccessScopes2'] =  _get_policy_data_access_scope2()
    payload["user"]['payRuleScriptSchedule'] = _get_payrule_script_schedule_to_assign(dag_run)
    payload["user"]['displayNameParameter'] = {
            "displayName": f"""{dag_run.conf['file_data']['last_name']}, {dag_run.conf['file_data']['first_name']} {dag_run.conf['file_data']['emp_id']} {dag_run.conf['file_data']['email_id']}"""
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


# Add User TimeOff
def get_timeoff_assignment_payload(dag_run):
    return {
        "userUri": dag_run.conf['user_uri'],
        "timeOffTypeUris": rail.result("map_mapper_replicon_timeoff")
    }

def get_timeoff_assignment_payload_for_update_user(dag_run):
    return {
        "userUri": dag_run.conf['user_uri'],
        "timeOffTypeUris": rail.result("map_mapper_replicon_timeoff")
    }

# This needs to be optimized
def get_special_timeoff_policies_payload(dag_run):
    
    tenure = (get_replicon_date(dag_run.conf['continuous_start_date'],
                "date") - get_replicon_date(dag_run.conf['start_date'], "date")).days
    if tenure < 0:
        tenure = 0
    else:
        tenure+=1
    tenure /= 365

    current_policy_to_assign = False
    policy_set = []
    # we are ignoring the policy where the offset is less than tenure
    # which will not occur #! need to confirm with @SukumarPalo
    for policy in rail.result("get_default_special_timeoff_policy"):
        if policy['startOffset']['offSetValue'] > tenure:
            policy_set.append(
                {
                    "offset" : policy['startOffset']['offSetValue'],
                    "policy": policy['policySet'],
                    "first": "No"
                }
            )
        if policy['startOffset']['offSetValue'] == tenure:
            current_policy_to_assign = True
            policy_set.append(
                {
                    "offset" : policy['startOffset']['offSetValue'],
                    "policy": policy['policySet'],
                    "first": "Yes"
                }
            )
            
        
    if not current_policy_to_assign:
        max_diff = null
        new_list = []
        for policy2 in rail.result("get_default_special_timeoff_policy"):
            if float(policy2['offset']) < float(tenure):
                if not max_diff:
                    max_diff = policy2['offset'] - tenure
                else:
                    if max_diff < policy2['offset'] - tenure:
                        max_diff = policy2['offset'] - tenure
                policy2['diff'] = policy2['offset'] - tenure
                new_list.append(
                    {
                        **policy2,
                        **{
                            'diff': policy2['offset'] - tenure
                        }
                    }
                )

        if max_diff:
            for policy3 in new_list:
                if float(policy3['diff']) == float(tenure):
                    policy_set.append({
                        **policy3
                        **{
                            "first" : "Yes"   
                        }
                        }
                    )
                    
        policy_to_assign = []
        continuous_start_date = get_replicon_date(dag_run.conf['continuous_start_date'], "date")
        for policy4 in policy_set:
            if policy4['first'] == "Yes":
                policy_to_assign.append(
                    {
                        "Description" : f"Effective on {dag_run.conf['start_day_json']['day']}/{dag_run.conf['start_day_json']['month']},{dag_run.conf['start_day_json']['year']}",
                        "effectiveDate": dag_run.conf['start_day_json'],
                        "policySet": policy4['policy']
                    }
                )
            else:
                effective_date = continuous_start_date + relativedelta(year=int(policy4['offset']))
                policy_to_assign.append(
                    {
                        "Description" : f"Effective on {effective_date.day}{effective_date.month},{effective_date.year}",
                        "effectiveDate":{
                            "year": effective_date.year,
                            "month": effective_date.month,
                            "day": effective_date.day
                        },
                        "policySet": policy4['policy']
                    }
                )

    return loads(policy_to_assign).replace("'script'" ,"scriptTarget") 


def get_update_timeoff_policies_payload(dag_run):

    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['user_uri'],
            "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
        },
        "policySetScheduleEntries": loads(dumps(rail.result("get_default_timeoff_policy")
                    ).replace("null", "\"effective\""
                ).replace("\"script\"", "\"scriptTarget\""
                )) if rail.result("get_default_timeoff_policy") else []
    }

def get_update_timeoff_policies_payload_update_user(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['user_uri'],
            "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
        },
        "policySetScheduleEntries": loads(dumps(rail.result("get_default_timeoff_policy")
                    ).replace("null", "\"effective\""
                ).replace("\"script\"", "\"scriptTarget\""
                ))
    }

def get_custom_fields_payload(uri, txt_value=null, date_value=null, drop_down_value=null, number_value=null):
    return {
        "customField": {
            "uri": uri,
            "name": null,
            "groupUri": null
        },
        "text": txt_value,
        "date": date_value,
        "dropDownOption": {
            "uri": null,
            "name": null
            } if drop_down_value else null,
        "number": number_value
    }

def update_txt_udf(dag_run, input_field_name, udf_display_text_value, udf_key_name, custom_fields_payload, current_custom_fields_values):
    if dag_run.conf['file_data'][input_field_name]:
        if dag_run.conf['file_data'][input_field_name] != rail.find_first_by_attr_and_get_attr(
            current_custom_fields_values, "customField.displayText", udf_display_text_value, 'text', default=""):
                    custom_fields_payload.append(
                        get_custom_fields_payload(uri=dag_run.conf['udfs'][udf_key_name].get('uri'), txt_value=dag_run.conf['file_data'][input_field_name]))
                    if input_field_name == "management_lvl":
                        if dag_run.conf['file_data']['management_lvl'] in ['L1', 'L2']:
                            return True
                        return False
                    
                        

def update_date_udf(dag_run, input_field_name, json_formatted_date_field_name, udf_display_text_value, udf_key_name, custom_fields_payload, current_custom_fields_values):
    if dag_run.conf['file_data'][input_field_name]:
        if compare_if_two_json_dates_are_same(dag_run.conf['json_formatted_dates'][json_formatted_date_field_name],
                                                rail.find_first_by_attr_and_get_attr(current_custom_fields_values,
                                                                                    "customField.displayText", udf_display_text_value, 'date', default="")):
                    custom_fields_payload.append(
                        get_custom_fields_payload(uri=dag_run.conf['udfs'][udf_key_name].get('uri'), date_value=dag_run.conf['json_formatted_dates'][json_formatted_date_field_name]))

def get_user_custom_field_update_payload(dag_run, user_details):
    custom_fields_payload = []
    current_custom_fields_values = user_details['customFieldValues']
    update_txt_udf(dag_run, 'assignment_type', 'assignment_type', 'assignment_type', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'perner_id', 'IA PERNER ID', 'ia_perner_id', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'gender', 'Gender', 'gender', custom_fields_payload, current_custom_fields_values)
    can_update_notifications = update_txt_udf(dag_run, 'management_lvl', 'Management Level', 'management_level', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'on_leave', 'On Leave', 'on_leave', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'area_code', 'Personnel Area Code', 'personnel_area_code', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'area_name', 'Personnel Area Description', 'personnel_area_name', custom_fields_payload, current_custom_fields_values)
    # For canada has some custom logic but in GBL setup canada country will not come
    update_txt_udf(dag_run, 'job_level', 'Job Activity Type', 'job_level', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'fte', 'FTE', 'fte', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'fte_pct', 'FTE %', 'ftepct', custom_fields_payload, current_custom_fields_values)
    update_txt_udf(dag_run, 'is_ia', 'International Assignee', 'international_assignee', custom_fields_payload, current_custom_fields_values)
    # is_is_start_date
    ia_updated, ia_exception_msg, effective_date = get_ia_update_payload_for_udf_update(dag_run, custom_fields_payload=custom_fields_payload, 
                                    current_custom_fields_values=current_custom_fields_values, update_txt_udf=update_txt_udf, update_date_udf=update_date_udf)
    rail.set_result(key="ia_updated", val=False)
    rail.set_result(key="ia_exception_msg", val="")   
                
    update_date_udf(dag_run, 'service_date', 'service_date', 'Continuous Service Date', 'service_date', custom_fields_payload, current_custom_fields_values)
    update_date_udf(dag_run, 'ia_start_date', 'ia_start_date', 'International assignee start date', 'international_assignee_start_date', custom_fields_payload, current_custom_fields_values)
    update_date_udf(dag_run, 'ia_end_date', 'ia_end_date', 'International assignee end date', 'international_assignee_end_date', custom_fields_payload, current_custom_fields_values)

    return custom_fields_payload, can_update_notifications, null


def get_two_date_diff(effective_date, user_start_date, today):
    if effective_date:
        return convert_json_date_to_date(today) - convert_json_date_to_date(effective_date)
    return convert_json_date_to_date(today) -  convert_json_date_to_date(user_start_date)

def get_current_payrule_schedule_timesheetPeriod(payrule_schedule_details, user_start_date):
    current_effective_payrule = None
    # as an identifier to process very 1st record
    #! can be optimized
    current_min_day_diff = "*"
    today= get_todays_date_in_json()
    # iter from 2nd item as we have considered the 1st record as current
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

def get_effective_date_based_on_work_week(work_week, work_week_starts_with_check:list):
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

def prepare_update_user_payload_callable(dag_run, config):
    today= get_todays_date_in_json()
    payload = {
        "user": {
            "uri": dag_run.conf['user_uri']
        },
        "modifications": {},
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
    can_update_display_name = False
    logger = []
    _get_user_details = rail.result("get_user_details")
    user_details = _get_user_details['userDetails']
    payload['modifications']['userDetailsToApply'] = {}
    if dag_run.conf['file_data']['first_name'] and dag_run.conf['file_data']['first_name'].lower() != user_details['firstName'].lower():
        payload['modifications']['userDetailsToApply']['firstName'] = dag_run.conf['file_data']['first_name']
        can_update_display_name = True
    if dag_run.conf['file_data']['last_name'] and dag_run.conf['file_data']['last_name'].lower() != user_details['lastName'].lower():
        payload['modifications']['userDetailsToApply']['lastName'] = dag_run.conf['file_data']['last_name']
        can_update_display_name = True
    if config.instance in ["prod", "trial"]:
        if dag_run.conf['file_data']['email_id']:
            if _get_user_details['securityConfiguration']['loginName'] !=  dag_run.conf['file_data']['email_id']:
                payload['modifications']['securitySettingsToApply'] = {
                    "loginEnabled": "true",
                    "forcePasswordChange": "false",
                    "loginName": dag_run.conf['file_data']['email_id'],
                    "sso": dag_run.conf['file_data']['email_id'],
                    "password": None,
                    "enabledAuthenticationTypeUris": ["urn:replicon:user-authentication-type:sso"],
                    "userSSONameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name"
                }

                payload['modifications']['userDetailsToApply']['emailAddress'] = {"emailAddress": dag_run.conf['file_data']['email_id']}
                can_update_display_name = True
            if not user_details['emailAddress']:
                payload['modifications']['userDetailsToApply']['emailAddress'] = {"emailAddress": dag_run.conf['file_data']['email_id']}
                can_update_display_name = True
    if can_update_display_name:
        payload['modifications']['userDetailsToApply']['displayNameParameter'] = {}
        payload['modifications']['userDetailsToApply']['displayNameParameter'][
            'displayName'] = f"{dag_run.conf['file_data']['last_name']},{dag_run.conf['file_data']['first_name']} {dag_run.conf['file_data']['emp_id']} {dag_run.conf['file_data']['email_id']}"
    if not payload['modifications']['userDetailsToApply']:
        payload['modifications']['userDetailsToApply'] = null
    custom_field_update_payload, can_update_notification_pref, effective_date = get_user_custom_field_update_payload(dag_run, user_details)
    effective_date = null
    payload['modifications']['customFieldValuesToApply'] = custom_field_update_payload
    
    
    if dag_run.conf['payrule']['payrule']:
        current_payrule_schedule = get_current_payrule_schedule_timesheetPeriod(_get_user_details['payRuleScriptSchedule'], user_details['employmentDateRange']['startDate'])
        if (not current_payrule_schedule or (current_payrule_schedule['payRuleScript']['displayText'] != dag_run.conf['payrule']['payrule'])):
            if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
                payrule_effective_date = get_effective_date_based_on_work_week(dag_run.conf['mapper_data']['work_week'], ['saturday'])
                payload['modifications']['payRulesScheduleModifications'] = {
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
                            } if not effective_date else effective_date
                        }
                    ]
                }
                rail.set_result(key="payrule_updated", val="Yes")

    if dag_run.conf['file_data']['work_shift']:
        current_office_schedule = get_current_payrule_schedule_timesheetPeriod(_get_user_details['schedulePolicies'], user_details['employmentDateRange']['startDate'])
        if (not current_office_schedule) or (not current_office_schedule['officeSchedule']) or (current_office_schedule['officeSchedule']['displayText'] != dag_run.conf['file_data']['work_shift']):
            if dag_run.conf['schedule_data']['work_schedule_uri']:
                payload['modifications']['schedulePolicyToApply'] = {
                    "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementSchedule": [],
                    "updateScheduleOverDateRange": {
                        "replacementScheduleEntries": [
                            {
                                "schedulePolicy": {
                                    "officeScheduleUri": null,
                                    "name": dag_run.conf['file_data']['work_shift'],
                                    "officeSchedule": {
                                        "officeScheduleUri": null,
                                        "name": dag_run.conf['file_data']['work_shift']
                                    },
                                    "scheduleTypeUri": dag_run.conf['mapper_data']['schedule_type_uri']
                                },
                                "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['work_shift_effective_date']
                            }
                        ],
                        "endDate": null
                    }
                }
            else:
                logger.append(f"Office schedule {dag_run.conf['file_data']['work_shift']} not available in Replicon")
        
    if dag_run.conf['mapper_data']['time_entry_approval_path_name']:
        current_timeentry_approval_path = rail.result("get_time_entry_approval_path")
        if not current_timeentry_approval_path or current_timeentry_approval_path['displayText'] != dag_run.conf['mapper_data']['time_entry_approval_path_name']:
            payload['modifications']['timeEntryRevisionGroupApprovalPathToApply'] = {
                "uri": null,
                "name":  dag_run.conf['mapper_data']['time_entry_approval_path_name']
            }
    
    if _get_user_details['timesheetPeriodSchedule']:
        current_timesheet_period = get_current_payrule_schedule_timesheetPeriod(_get_user_details['timesheetPeriodSchedule'],
                                                                                 user_details['employmentDateRange']['startDate'])

        if dag_run.conf['mapper_data']['timesheet_period'] and dag_run.conf['mapper_data']['timesheet_period'] != current_timesheet_period['timesheetPeriod']['displayText']:
            timesheet_effective_date = get_effective_date_based_on_work_week(dag_run.conf['mapper_data']['work_week'], ['saturday', 'sunday'])
            payload['modifications']['timesheetPeriodScheduleToApply'] = {
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
                                "day": timesheet_effective_date.day,
                                "month": timesheet_effective_date.month,
                                "year": timesheet_effective_date.year
                            }
                        }
                    ],
                    "endDate": null
                }
            }

    current_effective_grps = rail.result("get_effective_group_membership")
    if dag_run.conf['file_data']['org_code'] and dag_run.conf['file_data']['org_code'] != current_effective_grps['department'].get('displayText', ''):
        payload['modifications']['departmentGroupScheduleToApply'] = {
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
                        "effectiveDate": effective_date if effective_date else today
                    }
                ],
                "endDate": null
            }
            }

    if dag_run.conf['groups']['location'] and dag_run.conf['groups']['location'].get('uri' ,'') != current_effective_grps['location'].get('uri' ,''):
        payload['modifications']['locationScheduleToApply'] = {
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

    if dag_run.conf['file_data']['cost_center'] and dag_run.conf['file_data']['cost_center'] != current_effective_grps['costCenter'].get('displayText', ''):
        payload['modifications']['costCenterScheduleToApply'] = {
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

    if dag_run.conf['groups']['division'].get('uri' ,'') and dag_run.conf['groups']['division'].get('uri' ,'') != current_effective_grps['division'].get('uri' ,''):
        payload['modifications']['divisionScheduleToApply'] = {
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

    if dag_run.conf['groups']['employee_type'] and dag_run.conf['groups']['employee_type']['uri'] != current_effective_grps['employeeType'].get('uri', ''):
        employee_type_uri = dag_run.conf['groups']['employee_type']['uri']
        payload['modifications']['employeeTypeGroupScheduleToApply'] = {
            "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementEmployeeTypeGroupSchedule": [],
            "updateEmployeeTypeGroupScheduleOverDateRange": {
                "replacementEmployeeTypeGroupScheduleEntries": [
                    {
                        "employeeTypeGroup": {
                            "uri": employee_type_uri,
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

    if dag_run.conf['file_data']['pay_group'] and dag_run.conf['file_data']['pay_group'] != current_effective_grps['serviceCenter'].get('displayText', ''):
        payload['modifications']['serviceCenterScheduleToApply'] = {
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
                    "effectiveDate": effective_date if effective_date else today
                }
                ],
                "endDate": null
            }
        }

    if dag_run.conf['timezone']['timezone']:
        if dag_run.conf['timezone']['timezone_uri'] != _get_user_details['timeZone']['uri']:
            payload['modifications']['timezoneToApply'] = {
                "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": {
                    "uri": dag_run.conf['timezone']['timezone_uri'],
                    "IANAName": null
                }
            }

    else:
        logger.append(f"Timezone not defined in mapper for Location {dag_run.conf['file_data']['country']}")

    if dag_run.conf['activities']['activity']:
        activity_list = dag_run.conf['activities']['activity'].split('|')
        user_activities = _get_user_details['assignedActivities']
        can_assign_activities = False
        for activity in user_activities:
            if activity['name'] in activity_list:
                can_assign_activities = True
                break
        if can_assign_activities:
            payload['modifications']['activitiesToApply'] = list(map(lambda _activity: {
                    "uri": null,
                    "name": _activity,
                }, activity_list))

    if is_profile_enabled(dag_run):
        # above timesheetPeriod assignment may get overwritten
        current_timesheet_period2 = _get_user_details['timesheetPeriodSchedule'][0].get(
            'timesheetPeriod', {}).get('displayText') if _get_user_details['timesheetPeriodSchedule'] else []
        if (not current_timesheet_period2) and dag_run.conf['json_formatted_dates'][
            'timesheet_period_effective_date'] and convert_json_date_to_date(
                dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']) > convert_json_date_to_date(
                dag_run.conf['json_formatted_dates']['hire_date']):
            payload['modifications']['timesheetPeriodScheduleToApply'] = {
                "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementTimesheetPeriodSchedule": [],
                "updateTimesheetPeriodScheduleOverDateRange": {
                    "replacementTimesheetPeriodScheduleEntries": [
                        {
                            "timesheetPeriod": {
                                "uri": null,
                                "name": dag_run.conf['mapper_data']['timesheet_period']
                            },
                            "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']
                        }
                    ],
                    "endDate": null
                }
            }

        if not current_timesheet_period2:
            if dag_run.conf['json_formatted_dates']['timesheet_period_effective_date'] and \
            convert_json_date_to_date(
                    dag_run.conf['json_formatted_dates']['hire_date']) > convert_json_date_to_date(
                    dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']):
                
                payload['modifications']['timesheetPeriodScheduleToApply'] = {
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
                payload['modifications']['timesheetPeriodScheduleToApply'] = {
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
        
        if dag_run.conf['mapper_data']['work_week_uri']:
            payload['modifications']['workWeekStartToApply'] = {
                "workWeekStartDayUri": dag_run.conf['mapper_data']['work_week_uri']
            }

        if dag_run.conf['mapper_data']['timeoff_approval']:
            payload['modifications']['timeOffApprovalPathToApply'] = {
                "uri": null,
                "name": dag_run.conf['mapper_data']['timeoff_approval']
            }
        
        payload['modifications']['timesheetApprovalPathToApply'] = {
            "uri": null,
            "name": dag_run.conf['mapper_data']['timesheet_approval_path']
        }

        timesheet_template = []
        holiday_calendar = []
        timeoff_template = []
    
    rail.set_result(key="can_update_notification_pref", val=can_update_notification_pref)
    rail.set_result(key="exception_log", val=logger)
    return payload
