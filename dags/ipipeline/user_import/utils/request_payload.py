import rail
import pendulum
from uuid import uuid4
from ipipeline.user_import.utils import custom_methods
from datetime import datetime
from airflow.models import Variable

null = None
true = True
false = False

def get_changed_records_query(can_use_reference_file_var_name):
    if Variable.get(can_use_reference_file_var_name, default_var='true').lower() == 'true':
        return """SELECT c.* FROM current_data c WHERE c.hash_sha256 NOT IN (SELECT DISTINCT r.hash_sha256 FROM reference_data r)"""
    return """SELECT c.* FROM current_data c"""

def get_user_creation_payload(dag_run, config):
    """
    Build payload for creating a new user in iPipeline format following tsystems pattern
    """
    user_data = dag_run.conf

    # Parse dates
    start_date = rail.parse_date(user_data.get('start_date'), config.REP_DATE_FORMAT) if user_data.get('start_date') else null
    end_date = rail.parse_date(user_data.get('end_date'), config.REP_DATE_FORMAT) if user_data.get('end_date') else null
    
    # Extract project role data once for reuse
    project_role_data = user_data.get('calculated_project_role_data')

    schedule_manager_supervisor_uri = dag_run.conf.get("calculated_schedule_manager_supervisor_permission")
    schedule_manager_not_supervisor_uri = dag_run.conf.get("calculated_schedule_manager_not_supervisor_permission")

    schedule_manager_permissions = get_schedule_manager_permission(dag_run, schedule_manager_supervisor_uri, schedule_manager_not_supervisor_uri, config.defaults_mapper_data, is_update=False)
    
    # Determine login status - check if end date is in past
    login_enabled = true
    
    # If end date is in the past, disable the user regardless of calculated_login_status
    if user_data.get('end_date'):
        end_date_obj = datetime.strptime(user_data.get('end_date'), config.REP_DATE_FORMAT)
        current_date_obj = pendulum.now(config.time_zone).date()
        
        if end_date_obj.date() <= current_date_obj:
            login_enabled = false

    weekday = user_data.get('calculated_work_week_start_day').split('-')[0] if user_data.get('calculated_work_week_start_day') else null

    payload = {
        "target": null,
        "template": null,
        "modifications": {
            "firstName": {
                "value": user_data.get('first_name')
            },
            "lastName": {
                "value": user_data.get('last_name') or config.defaults_mapper_data['last_name_fallback']
            },
            "loginName": {
                "value": user_data.get('login_name')
            },
            "displayName": {
                "value": user_data.get('display_name')
            },
            "emailAddress": {
                "value": user_data.get('email')
            },
            "employeeId": {
                "value": user_data.get('employee_id')
            },
            "employmentDateRange": {
                "value": {
                    "startDate": start_date,
                    "endDate": end_date,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": login_enabled
                    },
                    "forcePasswordChange": null,
                    "ssoName": {
                        "value": user_data.get('authentication_id')
                    },
                    "ssoNameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name",
                    "password": null,
                    "authenticationProviders": [],
                    "emailMFAResendVerificationEmail": null,
                    "emailMFATryAddMethodFromUsersEmail": null,
                    "isMFAMethodRequired": null,
                    "clearIsLockedOut": null
                }
            },
            "timesheetApprovalPath": {
                "value": {
                    "uri": null,
                    "name": user_data.get('calculated_timesheet_approval_path')
                }
            } if user_data.get('calculated_timesheet_approval_path') else null,
            "timeEntryApprovalPath": {
                "value": {
                    "uri": null,
                    "name": user_data.get('calculated_time_entry_approval_path')
                }
            } if user_data.get('calculated_time_entry_approval_path') else null,
            "workAuthorizationApprovalPath": null,
            "timeoffApprovalPath": {
                "value": {
                    "uri": null,
                    "name": user_data.get('calculated_time_off_approval')
                }
            } if user_data.get('calculated_time_off_approval') else null,
            "timeOffBalancePayoutApprovalPath": null,
            "defaultActivity": null,
            "expenseApprovalPath": null,
            "timeZone": null,
            "workWeekStartDay": {
                "value": {
                    "uri": f"urn:replicon:day-of-week:{weekday.lower()}",
                }
            } if weekday else null,
            "defaultBillingRate": null,
            "userPreferences": null,
            "formattings": null,
            "notificationPreferences": {
                "value": get_notification_preferences(dag_run)
            } if get_notification_preferences(dag_run) is not null else null,
            "timesheetPeriodSchedule": [],
            "timesheetTemplate": {
                "value": {
                    "uri": user_data.get('calculated_timesheet_template_uri'),
                    "name": null
                }
            } if user_data.get('calculated_timesheet_template_uri') else null,
            "timeoffTemplate": {
                "value": {
                    "uri": null,
                    "name": user_data.get('calculated_time_off_template')
                }
            } if user_data.get('calculated_time_off_template') else null,
            "timeOffCalendarVisibility": null,
            "expenseTemplate": null,
            "workAuthorizationTemplate": null,
            "punchEntryPolicy": null,
            "holidayCalendarSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": user_data.get('calculated_holiday_calendar_uri'),
                        "name": null
                    }
                }
            ] if user_data.get('calculated_holiday_calendar_uri') else null,
            "extensionFields": get_user_oefs_for_add_update(dag_run, config.oef_field_mapper_data, is_update=false)["payload"],
            "customFields": [],
            "products": [],
            "skills": [],
            "activities": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "uri": user_data.get("calculated_activities", {}).get(activity),
                            "name": null
                        } for activity in user_data.get("calculated_activities", {}).keys()
                    ]
                }
            ] if user_data.get("calculated_activities") else [],
            "policySets": [],
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "uri": permission_uri,
                                "name": null
                            },
                            "groupAccessFilter": null
                        }
                        for permission_name, permission_uri in user_data.get("calculated_permissions", {}).items() if permission_uri
                    ] + ([schedule_manager_permissions] if schedule_manager_permissions else [])
                }
            ] if (user_data.get("calculated_permissions") and any(v for v in user_data.get("calculated_permissions", {}).values())) or schedule_manager_permissions else [],
            "bankedTimePolicies": [],
            "timeOffTypes": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "timeOffType": {
                                "uri": timeoff_config.get("uri")
                            },
                            "isTimeOffAllowedAgainstThisTimeOffType": timeoff_config.get("visible_to_employees"),
                            "applyDefaultTimeOffTypePolicy": false,
                            "defaultTimeOffTypePolicyEffectiveDate": null,
                            "policySchedule": []
                        } for timeoff_config in dag_run.conf.get("calculated_time_off_types", {}).values()
                    ]
                }
            ],
            "locationSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": user_data.get("calculated_location_uri"),
                        "parentUri": null,
                        "name": null
                    }
                }
            ] if user_data.get("calculated_location_uri") else [],
            "divisionSchedule": [],
            "costCenterSchedule": [],
            "serviceCenterSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": user_data.get("calculated_orgrole_uri"),
                        "parentUri": null,
                        "name": null
                    }
                }
            ] if user_data.get("calculated_orgrole_uri") else [],
            "departmentGroupSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": user_data.get("calculated_department_uri"),
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    }
                }
            ] if user_data.get("calculated_department_uri") else [],
            "employeeTypeGroupSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": user_data.get("calculated_employee_type_uri"),
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    }
                }
            ] if user_data.get("calculated_employee_type_uri") else [],
            "supervisorSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "loginName": user_data.get('supervisor'),
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                }
            ] if user_data.get('supervisor') and rail.result("get_supervisor_details") else [],
            "timesheetPeriodSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": user_data.get('calculated_timesheet_period_uri'),
                        "name": null
                    }
                }
            ] if user_data.get('calculated_timesheet_period_uri') else [],
            "holidayCalendarSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": user_data.get('calculated_holiday_calendar_uri'),
                        "name": null
                    }
                }
            ] if user_data.get('calculated_holiday_calendar_uri') else [],
            "scheduleTypeSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "scheduleTypeUri": user_data.get('calculated_schedule_type_uri'),
                        "officeSchedule": {
                            "officeScheduleUri": user_data.get('calculated_office_schedule_uri'),
                            "name": null
                        } if "office-schedule" in user_data.get('calculated_schedule_type_uri') else null
                    }
                }
            ] if user_data.get('calculated_schedule_type_uri') and not ("office-schedule" in user_data.get('calculated_schedule_type_uri') and not user_data.get('calculated_office_schedule_uri')) else [],
            "payRuleSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": user_data.get('calculated_payrule_uri'),
                        "name": null
                    }
                }
            ] if user_data.get('calculated_payrule_uri') else [],
            "placeSchedule": [],
            "payRateSchedule": [],
            "projectRoleSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "projectRole": {
                            "uri": user_data.get("calculated_project_role_uri"),
                            "name": null
                        },
                        "isPrimary": "true"
                    }
                }
            ] if user_data.get("calculated_project_role_uri") else [],
            "costNormalizationRuleSchedule": [],
            "hourlyRatesSchedule": [],
            "substituteUserSchedule": [],
            "policySetsScheduleToApply": [
                {
                    "policyUri": "urn:replicon:policy:timesheet",
                    "schedule": [
                        {
                            "policySetUri": user_data.get("calculated_timesheet_template_uri"),
                            "effectiveDate": null
                        }
                    ]
                }
            ] if user_data.get("calculated_timesheet_template_uri") else []
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }
    
    return payload

def get_update_user_req(dag_run, config):
    current_date = rail.parse_date(dag_run.conf["current_date"], config.YMD_DATE_FORMAT)
    
    # Initialize group membership result for reuse across different update sections
    group_membership_result = null
    
    # Track if we have any modifications
    has_modifications = false
    
    # Get basic user details modifications (including start/end dates)
    basic_details_result = get_basic_user_details_update(dag_run, config.YMD_DATE_FORMAT, config.REP_DATE_FORMAT)
    modifications = basic_details_result["modifications"]
    
    # Check if basic details have any modifications
    if modifications:
        has_modifications = true
    
    # Check for location update
    location_update = get_updated_location(dag_run)
    if location_update:
        has_modifications = true
        # Check if user has existing location - if no existing location, pass null for startDate/endDate
        if not group_membership_result:
            group_membership_result = rail.result("get_current_group_membership", {})
        existing_location_uri = group_membership_result.get("existinglocationuri") if group_membership_result else null
        
        modifications["locationSchedule"] = [{
            "dateRange": {
                "startDate": current_date if existing_location_uri else null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "item": {
                "uri": location_update,
                "parentUri": null,
                "name": null
            }
        }]
    
    # Check for department update
    department_update = get_updated_department(dag_run)
    if department_update:
        has_modifications = true
        # Check if user has existing department - if no existing department, pass null for startDate/endDate
        if not group_membership_result:
            group_membership_result = rail.result("get_current_group_membership", {})
        existing_department_uri = group_membership_result.get("existingdepartmenturi") if group_membership_result else null
        
        modifications["departmentGroupSchedule"] = [{
            "dateRange": {
                "startDate": current_date if existing_department_uri else null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "item": {
                "uri": department_update,
                "parent": null,
                "name": null,
                "parameterCorrelationId": null
            }
        }]
    
    # Check for employee type update
    employee_type_update = get_updated_employeetype(dag_run)
    if employee_type_update:
        has_modifications = true
        # Check if user has existing employee type - if no existing employee type, pass null for startDate/endDate
        if not group_membership_result:
            group_membership_result = rail.result("get_current_group_membership", {})
        existing_employee_type_uri = group_membership_result.get("existingemployeetypeuri") if group_membership_result else null
        
        modifications["employeeTypeGroupSchedule"] = [{
            "dateRange": {
                "startDate": current_date if existing_employee_type_uri else null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "item": {
                "uri": employee_type_update,
                "parent": null,
                "name": null,
                "parameterCorrelationId": null
            }
        }]
    
    # Check for service center update
    servicecenter_update = get_updated_servicecenter(dag_run)
    if servicecenter_update:
        has_modifications = true
        # Check if user has existing service center - if no existing service center, pass null for startDate/endDate
        if not group_membership_result:
            group_membership_result = rail.result("get_current_group_membership", {})
        existing_servicecenter_uri = group_membership_result.get("existingservicecenteruri") if group_membership_result else null
        
        modifications["serviceCenterSchedule"] = [{
            "dateRange": {
                "startDate": current_date if existing_servicecenter_uri else null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "item": {
                "uri": servicecenter_update,
                "parentUri": null,
                "name": null
            }
        }]
    
    # Check for supervisor update
    supervisor_update = get_updated_supervisor(dag_run)
    if supervisor_update:
        has_modifications = true
        # Check if user has existing supervisor - if no existing supervisor, pass null for startDate/endDate
        supervisor_details = rail.result("get_supervisor_assignment_details", "supervisor")
        existing_supervisor = supervisor_details.get("user", {}).get("uri") if supervisor_details else null
        
        modifications["supervisorSchedule"] = [{
            "dateRange": {
                "startDate": current_date if existing_supervisor else null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "item": {
                "uri": supervisor_update,
                "loginName": null,
                "employeeId": null,
                "parameterCorrelationId": null
            }
        }]

    activities_update = get_updated_activities(dag_run)
    if activities_update:
        has_modifications = true

        # Build modifications structure
        modifications["activities"] = []

        if activities_update["uris_to_remove"]:
            modifications["activities"].append({
                "modificationOptionUri": "urn:replicon:collection-modification-option:remove",
                "items": [{"uri": uri} for uri in activities_update["uris_to_remove"]]
            })

        if activities_update["uris_to_add"]:
            modifications["activities"].append({
                "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                "items": [{"uri": uri} for uri in activities_update["uris_to_add"]]
            })
    
    # Check for holiday calendar update
    holiday_calendar_update = get_updated_holiday_calendar(dag_run)
    if holiday_calendar_update:
        has_modifications = true
        # Check if user has existing holiday calendar - if no existing holiday calendar, pass null for startDate/endDate
        current_holiday_calendar = rail.result("get_user_holiday_calendar", {})
        existing_holiday_calendar_uri = current_holiday_calendar.get("uri") if current_holiday_calendar else null
        
        modifications["holidayCalendar"] = null
        modifications["holidayCalendarSchedule"] = [{
            "dateRange": {
                "startDate": current_date if existing_holiday_calendar_uri else null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "item": {
                "uri": holiday_calendar_update,
                "name": null
            }
        }]
    
    # Check for timesheet template update
    timesheet_template_update = get_updated_timesheet_template(dag_run)
    if timesheet_template_update:
        has_modifications = true
        # Check if user has existing timesheet template - if exists, use current_date as effective date
        user_details_result = rail.result("get_user_details", {})
        existing_timesheet_template = user_details_result.get("timesheetTemplate") if user_details_result else null
        
        modifications["policySetsScheduleToApply"] = [
            {
                "policyUri": "urn:replicon:policy:timesheet",
                "schedule": [
                    {
                        "policySetUri": timesheet_template_update,
                        "effectiveDate": current_date if existing_timesheet_template else null
                    }
                ]
            }
        ]
    
    # Check for permission updates
    permission_updates = get_updated_permissions(dag_run, config.defaults_mapper_data)
    if permission_updates:
        has_modifications = true
        modifications["permissionSets"] = permission_updates
    
    # Check for schedule manager permission updates (independent of org role)
    schedule_manager_supervisor_uri = dag_run.conf.get("calculated_schedule_manager_supervisor_permission")
    schedule_manager_not_supervisor_uri = dag_run.conf.get("calculated_schedule_manager_not_supervisor_permission")
    schedule_manager_updates = get_schedule_manager_permission(dag_run, schedule_manager_supervisor_uri, schedule_manager_not_supervisor_uri, config.defaults_mapper_data, is_update=True)
    if schedule_manager_updates:
        has_modifications = true
        if not modifications.get("permissionSets"):
            modifications["permissionSets"] = []
        
        # Merge schedule manager permissions into existing permission sets
        for sm_update in schedule_manager_updates:
            sm_modification_uri = sm_update.get("modificationOptionUri")
            sm_items = sm_update.get("items", [])
            
            # Find existing permission set with same modificationOptionUri and append items
            found = False
            for perm_set in modifications["permissionSets"]:
                if perm_set.get("modificationOptionUri") == sm_modification_uri:
                    perm_set["items"].extend(sm_items)
                    found = True
                    break
            
            # If no matching modificationOptionUri found, add new permission set
            if not found:
                modifications["permissionSets"].append(sm_update)
    
    # Check for OEF updates
    oef_result = get_user_oefs_for_add_update(dag_run, config.oef_field_mapper_data, is_update=true)
    if oef_result["payload"]:
        has_modifications = true
        modifications["extensionFields"] = oef_result["payload"]
    
    
    # Handle basic timeoff types assignment (policies handled separately later)
    timeoff_changes = get_updated_timeoff_types(dag_run)
    
    # Only assign new timeoff types here - overlapping ones will be handled by separate policy tasks
    new_timeoff_types = timeoff_changes.get("new_timeoff_types", [])
    
    if new_timeoff_types:
        has_modifications = true
        
        assign_items = []
        for timeoff_uri in new_timeoff_types:
            # Get timeoff type visibility from calculated_time_off_types
            is_visible = true  # Default to true
            for type_name, type_config in dag_run.conf.get("calculated_time_off_types", {}).items():
                if type_config.get("uri") == timeoff_uri:
                    is_visible = type_config.get("visible_to_employees", true)
                    break
            
            assign_items.append({
                "timeOffType": {
                    "uri": timeoff_uri
                },
                "isTimeOffAllowedAgainstThisTimeOffType": is_visible,
                "applyDefaultTimeOffTypePolicy": false,  # Don't apply default policy here
                "defaultTimeOffTypePolicyEffectiveDate": null,
                "policySchedule": []  # Empty - policies handled separately later
            })
        
        modifications["timeOffTypes"] = [{
            "modificationOptionUri": "urn:replicon:collection-modification-option:add",
            "items": assign_items
        }]
    
    # Check for project role schedule update
    project_role_update = get_updated_project_role(dag_run)
    if project_role_update:
        has_modifications = true
        
        modifications["projectRoleSchedule"] = [{
            "dateRange": {
                "startDate": current_date if project_role_update["has_existing"] else null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "item": {
                "projectRole": {
                    "uri": project_role_update["new_uri"],
                    "name": null
                },
                "isPrimary": "true"
            }
        }]
    
    # Check for pay rule schedule update
    payrule_update = get_updated_payrule(dag_run)
    if payrule_update:
        has_modifications = true
        
        modifications["payRuleSchedule"] = [{
            "dateRange": {
                "startDate": current_date if payrule_update["has_existing"] else null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "item": {
                "uri": payrule_update["new_uri"],
                "name": null
            }
        }]
    
    # Check for schedule type update (office schedule)
    schedule_update = get_updated_schedule_type(dag_run)
    if schedule_update:
        has_modifications = true
        
        modifications["scheduleTypeSchedule"] = [{
            "dateRange": {
                "startDate": current_date if schedule_update["has_existing"] else null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "item": {
                "scheduleTypeUri": schedule_update["schedule_type_uri"],
                "officeSchedule": {
                    "officeScheduleUri": schedule_update["office_schedule_uri"],
                    "name": null
                } if "office-schedule" in schedule_update.get("schedule_type_uri") else null
            }
        }] if not ("office-schedule" in schedule_update.get("schedule_type_uri") and not schedule_update.get("office_schedule_uri")) else []
    
    # Check for timesheet period update
    timesheet_period_update = get_updated_timesheet_period(dag_run)
    if timesheet_period_update:
        has_modifications = true
        
        modifications["timesheetPeriodSchedule"] = [{
            "dateRange": {
                "startDate": current_date if timesheet_period_update["has_existing"] else null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "item": {
                "uri": timesheet_period_update["new_uri"],
                "name": null
            }
        }]
    
    # Check for work week start day update
    work_week_update = get_updated_work_week_start_day(dag_run)
    if work_week_update:
        has_modifications = true
        modifications["workWeekStartDay"] = {
            "value": {
                "uri": work_week_update
            }
        }
    
    # Check for timesheet approval path update
    timesheet_approval_update = get_updated_timesheet_approval_path(dag_run)
    if timesheet_approval_update:
        has_modifications = true
        modifications["timesheetApprovalPath"] = timesheet_approval_update
    
    # Check for time entry approval path update
    time_entry_approval_update = get_updated_time_entry_approval_path(dag_run)
    if time_entry_approval_update:
        has_modifications = true
        modifications["timeEntryApprovalPath"] = time_entry_approval_update
    
    # Check for time off approval path update
    time_off_approval_update = get_updated_time_off_approval_path(dag_run)
    if time_off_approval_update:
        has_modifications = true
        modifications["timeoffApprovalPath"] = time_off_approval_update
    
    # Check for time off template update
    timeoff_template_update = get_updated_timeoff_template(dag_run)
    if timeoff_template_update:
        has_modifications = true
        modifications["timeoffTemplate"] = {
            "value": {
                "uri": null,
                "name": timeoff_template_update
            }
        }
    
    # Add notification preferences based on calculated_timesheet_and_time_entry_notification
    notification_preferences = get_notification_preferences(dag_run, is_update=True)
    if notification_preferences is not null:
        has_modifications = true
        modifications["notificationPreferences"] = {
            "value": notification_preferences
        }
    
    # If no modifications found, return null
    if not has_modifications:
        return null
    
    # Build and return the complete payload
    return {
        "target": {
            "uri": rail.result("get_user_details", {}).get("userDetails", {}).get("uri") if rail.result("get_user_details", {}) else null,
        },
        "modifications": modifications,
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

# Add all the tsystems helper functions that are referenced above
def get_basic_user_details_update(dag_run, YMD_DATE_FORMAT, REP_DATE_FORMAT):
    """Build basic user details update payload per iPipeline tech spec"""
    basic_details_logs = []
    modifications = {}
    
    # Get current user details
    user_details_result = rail.result("get_user_details", {})
    current_user = user_details_result.get("userDetails", {}) if user_details_result else {}
    current_security = user_details_result.get("securityConfiguration", {}) if user_details_result else {}
    
    # Parse existing dates
    employment_date_range = current_user.get("employmentDateRange", {})
    replicon_start_date = employment_date_range.get("startDate") if employment_date_range else null
    existing_start_date = f'{replicon_start_date["year"]}/{replicon_start_date["month"]:02d}/{replicon_start_date["day"]:02d}' if replicon_start_date else null
    replicon_end_date = employment_date_range.get("endDate") if employment_date_range else null
    existing_end_date = f'{replicon_end_date["year"]}/{replicon_end_date["month"]:02d}/{replicon_end_date["day"]:02d}' if replicon_end_date else null
    
    # Current date for comparisons
    current_date_json = rail.parse_date(dag_run.conf["current_date"], YMD_DATE_FORMAT)
    current_date = datetime.strptime(dag_run.conf["current_date"], YMD_DATE_FORMAT)
    
    # Check basic fields
    first_name = dag_run.conf.get("first_name") or "unknown"
    last_name = dag_run.conf.get("last_name") or "unknown"
    display_name = dag_run.conf.get("display_name")
    
    # Update names
    if first_name != current_user.get("firstName"):
        modifications["firstName"] = {"value": first_name}
        basic_details_logs.append(f"First name updated")
    
    if last_name != current_user.get("lastName"):
        modifications["lastName"] = {"value": last_name}
        basic_details_logs.append(f"Last name updated")
    
    if display_name != current_user.get("customDisplayName"):
        modifications["displayName"] = {"value": display_name}
        basic_details_logs.append(f"Display name updated")
    
    if dag_run.conf.get("email") and dag_run.conf["email"] != current_user.get("emailAddress"):
        modifications["emailAddress"] = {"value": dag_run.conf["email"]}
        basic_details_logs.append(f"Email updated")
    
    # Handle employment dates and login status
    employment_date_changed = false
    employment_date_value = {}
    
    # Handle start date - REMOVED: No longer updating start dates
    start_date_changed = false
    
    # Handle end date
    end_date_changed = false
    if dag_run.conf.get("end_date"):
        end_date = datetime.strptime(dag_run.conf["end_date"], REP_DATE_FORMAT)
        start_date_str = dag_run.conf.get("start_date", existing_start_date)
        if start_date_str:
            start_date = datetime.strptime(start_date_str, REP_DATE_FORMAT if dag_run.conf.get("start_date") else REP_DATE_FORMAT)
            
            if end_date > start_date:
                new_end_date = rail.parse_date(dag_run.conf["end_date"], REP_DATE_FORMAT)
                new_end_str = f'{new_end_date["year"]}/{new_end_date["month"]:02d}/{new_end_date["day"]:02d}'
                
                if not existing_end_date or new_end_str != existing_end_date:
                    employment_date_value["endDate"] = new_end_date
                    end_date_changed = true
                    basic_details_logs.append(f"End date updated")
            else:
                basic_details_logs.append(f"End date '{dag_run.conf['end_date']}' not applied - must be after start date")
    
    # Only set employment_date_changed if end date actually changed (start date changes removed)
    employment_date_changed = end_date_changed
    
    # If end date changed, we need to include both dates to maintain the complete range
    if employment_date_changed:
        # Always include existing start date when updating end date
        employment_date_value["startDate"] = rail.parse_date(existing_start_date, YMD_DATE_FORMAT) if existing_start_date else null
        if not end_date_changed:
            employment_date_value["endDate"] = rail.parse_date(existing_end_date, YMD_DATE_FORMAT) if existing_end_date else null
    
    if employment_date_changed:
        employment_date_value["relativeDateRangeUri"] = null
        employment_date_value["relativeDateRangeAsOfDate"] = null
        modifications["employmentDateRange"] = {"value": employment_date_value}
    
    # If there's an end date, check if it's in the past to disable login
    if dag_run.conf.get("end_date"):
        end_date = datetime.strptime(dag_run.conf["end_date"], REP_DATE_FORMAT)
        if end_date <= current_date and current_security.get("isLoginEnabled"):
            # Past end date - disable login and modify login name
            modifications["securitySettings"] = {
                "value": {"loginEnabled": {"value": false}}
            }
            # Modify login name with end date suffix
            modifications["loginName"] = {"value": f'{dag_run.conf["email"]}_{dag_run.conf["end_date"]}'}
            basic_details_logs.append(f"Login status disabled (end date in past)")
            basic_details_logs.append(f"Login name modified with end date suffix")
    
    return {
        "modifications": modifications,
        "basic_details_logs": basic_details_logs
    }

def get_notification_preferences(dag_run, is_update=False):
    """
    Build notification preferences based on calculated_timesheet_and_time_entry_notification
    from assignment rules mapper.
    
    Args:
        dag_run: DAG run containing user data with calculated notification settings
        is_update: Boolean indicating if this is an update scenario
        
    Returns:
        Dictionary with notification preferences configuration or null if enabled/no changes needed
    """
    notification_setting = dag_run.conf.get('calculated_timesheet_and_time_entry_notification', 'Disable')
    
    # If Enable, return null (keep default notifications)
    if notification_setting == 'Enable':
        return null
    
    # For update scenarios, check if timesheet/time entry notifications are already disabled
    if is_update:
        current_preferences = rail.result("get_notification_preferences_for_user", {})
        if current_preferences:
            current_delivery_prefs = current_preferences.get("notificationDeliveryPreferences", [])
            
            # Check if both timesheet and time-entry-revision-group are already set to never-deliver
            timesheet_disabled = any(
                pref.get("objectTypeUri") == "urn:replicon:object-type:timesheet" and
                pref.get("notificationDeliveryOptionUri") == "urn:replicon:user-notification-delivery-option:never-deliver"
                for pref in current_delivery_prefs
            )
            
            time_entry_disabled = any(
                pref.get("objectTypeUri") == "urn:replicon:object-type:time-entry-revision-group" and
                pref.get("notificationDeliveryOptionUri") == "urn:replicon:user-notification-delivery-option:never-deliver"
                for pref in current_delivery_prefs
            )
            
            # If already disabled, no need to update
            if timesheet_disabled and time_entry_disabled:
                return null
    
    # If Disable (and update needed), return notification preferences to disable timesheet/time entry notifications
    return {
        "notificationDeliveryPreferences": [
            {
                "objectTypeUri": "urn:replicon:object-type:timesheet",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:time-entry-revision-group", 
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            }
        ],
        "sharedDeliveryPreferenceOptionUris": []
    }

# PWC-Style Group Creation Payload Functions

def get_target_for_hierarchy(parent_path):
    """
    Build target structure using PWC Global pattern.
    Used for all hierarchy types (departments, locations, employee types).
    
    Args:
        parent_path: String like "iPipeline/Department1" or "Country/City"
    
    Returns:
        Target structure for the hierarchy API
    """
    if not parent_path:
        return null
    
    # Split path and build nested parent structure like PWC
    path_parts = parent_path.split('/')
    parent = null
    
    for part in path_parts:
        parent = {
            'name': part,
            'parent': parent if parent else null
        }
    
    return {'parent': parent} if parent else null

def get_departments_hierarchy_payload(dag_run):
    """
    Build hierarchical payload using PWC Global pattern.
    Creates department hierarchy chain from parents + child configuration.
    """
    
    parents = dag_run.conf.get('parents', '')
    child = dag_run.conf.get('child', '')
    
    if not child:
        return {
            "hierarchy": [],
            "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
            "unitOfWorkId": str(uuid4())
        }
    
    hierarchy = []
    
    # Process each level in the child chain
    for level in child.split('/'):
        target = get_target_for_hierarchy(parents)
        hierarchy.append({
            'target': target,
            'modificationToApply': {
                'name': level,
                'isEnabled': true
            }
        })
        # Update parents path for next level
        parents += f"/{level}" if parents else level
    
    return {
        "hierarchy": hierarchy,
        "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

def get_locations_hierarchy_payload(dag_run):
    """
    Build hierarchical payload using PWC Global pattern.
    Creates location hierarchy chain from parents + child configuration.
    """
    
    parents = dag_run.conf.get('parents', '')
    child = dag_run.conf.get('child', '')
    
    if not child:
        return {
            "hierarchy": [],
            "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
            "unitOfWorkId": str(uuid4())
        }
    
    hierarchy = []
    
    # Process each level in the child chain
    for level in child.split('/'):
        target = get_target_for_hierarchy(parents)
        hierarchy.append({
            'target': target,
            'modificationToApply': {
                'name': level,
                'isEnabled': true
            }
        })
        # Update parents path for next level
        parents += f"/{level}" if parents else level
    
    return {
        "hierarchy": hierarchy,
        "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

def get_employee_types_hierarchy_payload(dag_run):
    """
    Build hierarchical payload using PWC Global pattern.
    Creates employee type hierarchy chain from parents + child configuration.
    Supports up to 3 levels: Category → Schedule → Type
    """
    
    parents = dag_run.conf.get('parents', '')
    child = dag_run.conf.get('child', '')
    
    if not child:
        return {
            "hierarchy": [],
            "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
            "unitOfWorkId": str(uuid4())
        }
    
    hierarchy = []
    
    # Process each level in the child chain (up to 3 levels total)
    for level in child.split('/'):
        target = get_target_for_hierarchy(parents)
        hierarchy.append({
            'target': target,
            'modificationToApply': {
                'name': level,
                'isEnabled': true
            }
        })
        # Update parents path for next level
        parents += f"/{level}" if parents else level
    
    return {
        "hierarchy": hierarchy,
        "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

def get_project_roles_payload(dag_run):
    """
    Build payload for project role creation using PWC-style pattern.
    Project roles are flat (no hierarchy), so we create individual roles.
    """
    
    # Get project role names from DAG config (passed from parallel trigger)
    project_role_name = dag_run.conf.get('project_roles_to_create', [])
    
    return {
        "target": null,
        "modifications": {
            "name": project_role_name,
            "billingRateScheduleToApply": null,
            "costRateScheduleToApply": null,
            "descriptionToApply": null,
            "isBillableToApply": true
        },
        "projectRoleModificationOptionUri": "urn:replicon:project-role-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

def get_updated_location(dag_run):
    return dag_run.conf["calculated_location_uri"] if (dag_run.conf.get("calculated_location_uri") and
        dag_run.conf["calculated_location_uri"] != rail.result("get_current_group_membership", {}).get("existinglocationuri")) else null

def get_updated_department(dag_run):
    return dag_run.conf["calculated_department_uri"] if (dag_run.conf.get("calculated_department_uri") and
        dag_run.conf["calculated_department_uri"] != rail.result("get_current_group_membership", {}).get("existingdepartmenturi")) else null

def get_updated_servicecenter(dag_run):
    return dag_run.conf["calculated_orgrole_uri"] if (dag_run.conf.get("calculated_orgrole_uri") and
        dag_run.conf["calculated_orgrole_uri"] != rail.result("get_current_group_membership", {}).get("existingservicecenteruri")) else null

def get_updated_employeetype(dag_run):
    if not dag_run.conf.get("calculated_employee_type_uri"):
        return null
    
    current_group_membership = rail.result("get_current_group_membership", {})
    current_employee_type_uri = current_group_membership.get("existingemployeetypeuri") if current_group_membership else null
    
    if dag_run.conf["calculated_employee_type_uri"] == current_employee_type_uri:
        return null
    
    return dag_run.conf["calculated_employee_type_uri"]

def get_updated_supervisor(dag_run):
    supervisor_id = dag_run.conf.get("supervisor")
    employee_id = dag_run.conf.get("employee_id")
    
    if not supervisor_id or supervisor_id == employee_id:
        return null
    
    # Get supervisor URI if supervisor exists in system
    supervisor_details_result = rail.result("get_supervisor_details")
    if not supervisor_details_result:
        return null
        
    supervisor_uri = supervisor_details_result["userDetails"]["uri"]
    
    # Check current supervisor assignment
    supervisor_assignment = rail.result("get_supervisor_assignment_details", key="supervisor") if rail.result("get_supervisor_assignment_details", key="supervisor") else null
    current_supervisor_uri = supervisor_assignment.get("user", {}).get("uri") if supervisor_assignment else null
    
    return supervisor_uri if supervisor_uri != current_supervisor_uri else null

def get_updated_activities(dag_run):
    """Check if activities need update and return URIs to add/remove"""
    if not (get_updated_location(dag_run) or get_updated_employeetype(dag_run)):
        return null
    
    calculated_activities = dag_run.conf.get("calculated_activities")
    if not calculated_activities:
        return null
    
    # Get current activities with their URIs
    user_details_result = rail.result("get_user_details", {})
    current_activities = user_details_result.get("assignedActivities", []) if user_details_result else []
    current_uris = {activity.get("uri") for activity in current_activities if activity.get("uri")}
    
    # Get new URIs from the calculated activities
    new_uris = set(calculated_activities.values())
    
    # Calculate which URIs to remove and add
    uris_to_remove = current_uris - new_uris
    uris_to_add = new_uris - current_uris
    
    # If no changes needed, return null
    if not uris_to_remove and not uris_to_add:
        return null
    
    return {
        "uris_to_add": uris_to_add,
        "uris_to_remove": uris_to_remove,
        "has_existing": len(current_uris) > 0
    }


def get_updated_holiday_calendar(dag_run):
    return (dag_run.conf["calculated_holiday_calendar_uri"] if dag_run.conf.get("calculated_holiday_calendar_uri")
        and dag_run.conf["calculated_holiday_calendar_uri"] != (rail.result("get_user_holiday_calendar", {}).get("uri")
            if rail.result("get_user_holiday_calendar", {}) else null)
            else null)

def get_updated_timesheet_template(dag_run):
    if not dag_run.conf.get("calculated_timesheet_template_uri"):
        return null
    
    user_details_result = rail.result("get_user_details", {})
    current_timesheet_template = user_details_result.get("timesheetTemplate", {}) if user_details_result else {}
    current_timesheet_template_uri = current_timesheet_template.get("uri") if current_timesheet_template else null
    
    if dag_run.conf["calculated_timesheet_template_uri"] == current_timesheet_template_uri:
        return null
    
    return dag_run.conf["calculated_timesheet_template_uri"]

def get_updated_permissions(dag_run, defaults_mapper_data):
    """Check for permission updates based on org role code changes"""
    new_orgrole_code = dag_run.conf.get("calculated_orgrole_code")
    if not new_orgrole_code:
        return []
    
    # Get org role data and find current code by URI
    org_role_data = rail.load_all_records(dag_run.conf.get("calculated_orgrole_data"))
    current_group_membership = rail.result("get_current_group_membership", {})
    current_servicecenter_uri = current_group_membership.get("existingservicecenteruri") if current_group_membership else null
    
    current_orgrole_code = null
    if current_servicecenter_uri and org_role_data:
        for role_data in org_role_data:
            if role_data.get("uri") == current_servicecenter_uri:
                current_orgrole_code = role_data.get("code")
                break
    
    # Only update permissions if org role code changed
    if current_orgrole_code == new_orgrole_code:
        return []
    
    calculated_permissions = dag_run.conf.get("calculated_permissions", {})
    if not calculated_permissions:
        return []
    
    user_details_result = rail.result("get_user_details", {})
    current_permissions = user_details_result.get("permissionSets", []) if user_details_result else []
    
    # Get schedule manager URIs to avoid overlap
    schedule_manager_supervisor_uri = dag_run.conf.get("calculated_schedule_manager_supervisor_permission")
    schedule_manager_not_supervisor_uri = dag_run.conf.get("calculated_schedule_manager_not_supervisor_permission")
    schedule_manager_uris = {schedule_manager_supervisor_uri, schedule_manager_not_supervisor_uri}
    
    permissions_to_add = []
    for perm_name, perm_uri in calculated_permissions.items():
        if perm_uri not in schedule_manager_uris:
            permissions_to_add.append({
                "permissionSetPolicy": {
                    "uri": perm_uri,
                    "name": null
                },
                "groupAccessFilter": null
            })
    
    schedule = []
    
    if permissions_to_add:
        schedule.append({
            "modificationOptionUri": "urn:replicon:collection-modification-option:add",
            "items": permissions_to_add
        })
    
    
    return schedule

def get_schedule_manager_permission(dag_run, schedule_manager_supervisor_uri, schedule_manager_not_supervisor_uri, defaults_mapper_data, is_update=False):
    """
    Get schedule manager permission assignment based on supervisor status.
    
    Args:
        dag_run: DAG run context
        is_update: True for update scenario, False for add scenario
        
    Returns:
        For add: Single permission dict for user creation payload
        For update: List of modification operations (add/remove)
    """
    # Get calculated URIs
    calculated_supervisor_uri = dag_run.conf.get("calculated_supervisor_permission")
    
    if not (schedule_manager_supervisor_uri and schedule_manager_not_supervisor_uri):
        return [] if is_update else null
    
    # Determine supervisor status
    has_supervisor_permission = False
    
    if is_update:
        # For update: check both current and calculated permissions
        user_details_result = rail.result("get_user_details", {})
        current_permissions = user_details_result.get("permissionSets", []) if user_details_result else []
        
        current_permission_uris = {
            perm_set.get("uri") for perm_set in current_permissions if perm_set.get("uri")
        }
        
        # Check if supervisor permission exists in current permissions
        has_supervisor_in_current = calculated_supervisor_uri in current_permission_uris if calculated_supervisor_uri else False
        
        # Check if supervisor permission exists in calculated permissions
        calculated_permissions = dag_run.conf.get("calculated_permissions", {})
        has_supervisor_in_calculated = bool(calculated_supervisor_uri and 
                                          any(uri == calculated_supervisor_uri for uri in calculated_permissions.values()))
        
        # If supervisor permission is present in ANY of user details OR calculated, assign supervisor schedule manager
        has_supervisor_permission = has_supervisor_in_current or has_supervisor_in_calculated
    else:
        # For add: check if user will have supervisor permission
        calculated_permissions = dag_run.conf.get("calculated_permissions", {})
        has_supervisor_permission = bool(calculated_supervisor_uri and 
                                       any(uri == calculated_supervisor_uri for uri in calculated_permissions.values()))
    
    # Determine target permission
    if has_supervisor_permission:
        target_permission_uri = schedule_manager_supervisor_uri
        target_permission_name = defaults_mapper_data.get("schedule_manager_supervisor_permission")
        remove_permission_uri = schedule_manager_not_supervisor_uri
    else:
        target_permission_uri = schedule_manager_not_supervisor_uri
        target_permission_name = defaults_mapper_data.get("schedule_manager_not_supervisor_permission")
        remove_permission_uri = schedule_manager_supervisor_uri
    
    # Get restrictions
    schedule_manager_permissions = dag_run.conf.get("calculated_schedule_manager_permissions", {})
    restrictions = schedule_manager_permissions.get(target_permission_name, {}).get("restrictions")
    
    # Build group access filter
    group_access_filter = {
        "locations": [
            {
                "location": {
                    "uri": loc.get("uri"),
                    "name": null
                },
                "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:explicit-group",
                "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:include-descendants"
            }
            for loc in restrictions.get("locations", [])
        ] if restrictions and restrictions.get("locations") else [],
        "departmentGroups": [
            {
                "departmentGroup": {
                    "uri": dept.get("uri"),
                    "name": null
                },
                "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:explicit-group",
                "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:include-descendants"
            }
            for dept in restrictions.get("departments", [])
        ] if restrictions and restrictions.get("departments") else []
    } if restrictions else null
    
    if is_update:
        # For update: check if user already has the correct schedule manager permission and restrictions
        modifications = []
        
        # Check if user already has the target permission
        user_has_target_permission = target_permission_uri in current_permission_uris
        
        # Check if location has changed (which affects restrictions)
        location_changed = bool(get_updated_location(dag_run))
        
        # If user has the permission but location changed, need to update restrictions
        if user_has_target_permission and location_changed:
            # Remove old permission and add with new restrictions
            modifications.append({
                "modificationOptionUri": "urn:replicon:collection-modification-option:remove",
                "items": [{
                    "permissionSetPolicy": {
                        "uri": target_permission_uri,
                        "name": null
                    }
                }]
            })
            
        # Add schedule manager permission if user doesn't have it OR if location changed
        if not user_has_target_permission or location_changed:
            modifications.append({
                "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                "items": [{
                    "permissionSetPolicy": {
                        "uri": target_permission_uri,
                        "name": null
                    },
                    "groupAccessFilter": group_access_filter
                }]
            })
        
        return modifications
    else:
        # For add: return single permission dict
        return {
            "permissionSetPolicy": {
                "uri": target_permission_uri,
                "name": null
            },
            "groupAccessFilter": group_access_filter
        }

def get_updated_timeoff_types(dag_run):
    """Compare current user timeoff types with calculated ones and return URIs for changes"""
    # Always check if timeoff types need updates based on calculated values from master
    # Master DAG already determines timeoff types based on location + employee category combination
    
    calculated_timeoff_types = dag_run.conf.get("calculated_time_off_types", {})
    if not calculated_timeoff_types:
        return {"new_timeoff_types": [], "all_timeoff_types": [], "disable_timeoff_types": []}
    
    user_details_result = rail.result("get_user_details", {})
    if not user_details_result:
        return {"new_timeoff_types": [], "all_timeoff_types": [], "disable_timeoff_types": []}

    timeoff_policy_summary = user_details_result.get("timeOffTypePolicySummary", {})
    current_timeoff_assignments = timeoff_policy_summary.get("policiesByTimeOffType", [])
    
    current_timeoff_dict = {}
    for policy in current_timeoff_assignments:
        timeoff_type = policy.get("timeOffType", {})
        type_name = timeoff_type.get("displayText")
        type_uri = timeoff_type.get("uri")
        is_allowed = policy.get("isTimeOffAllowedAgainstThisTimeOffType")
        if type_name and type_uri:
            current_timeoff_dict[type_name] = {
                "uri": type_uri,
                "policy": policy,
                "is_allowed": is_allowed
            }
    
    current_active_types = set()
    for type_name, type_info in current_timeoff_dict.items():
        if type_info["is_allowed"]:
            current_active_types.add(type_name)
    
    new_types = set(calculated_timeoff_types.keys())
    
    removed_types = current_active_types - new_types
    added_types = new_types - current_active_types
    overlapping_types = current_active_types & new_types  # Types that exist in both
    
    disable_uris = []
    for type_name in removed_types:
        if type_name in current_timeoff_dict:
            disable_uris.append(current_timeoff_dict[type_name]["uri"])
    
    new_uris = []
    for type_name in added_types:
        if type_name in calculated_timeoff_types:
            timeoff_config = calculated_timeoff_types[type_name]
            uri = timeoff_config.get("uri")
            new_uris.append(uri)
    
    overlapping_uris = []
    for type_name in overlapping_types:
        if type_name in calculated_timeoff_types:
            timeoff_config = calculated_timeoff_types[type_name]
            uri = timeoff_config.get("uri")
            overlapping_uris.append(uri)
    
    all_new_uris = []
    for type_name in calculated_timeoff_types:
        timeoff_config = calculated_timeoff_types[type_name]
        uri = timeoff_config.get("uri")
        all_new_uris.append(uri)
    return {
        "new_timeoff_types": new_uris,
        "overlapping_timeoff_types": overlapping_uris,
        "all_timeoff_types": all_new_uris,
        "disable_timeoff_types": disable_uris
    }

def get_user_oefs_for_add_update(dag_run, oef_field_mapper_data, is_update=false):
    """Build OEF payload for add or update operations"""
    oefs = []
    missing_oefs = []
    missing_oef_tags = []
    updated_oefs = []

    user_data = dag_run.conf
    processed_oef_data = dag_run.conf.get('oef_data', {})
    
    current_oef_values = {}
    if is_update:
        user_details_result = rail.result("get_user_details", {})
        if user_details_result and user_details_result.get("userDetails"):
            current_extension_fields = user_details_result.get("userDetails", {}).get("extensionFieldValues", [])
            for field in current_extension_fields:
                oef_uri = field.get("definition", {}).get("uri")
                if oef_uri:
                    current_oef_values[oef_uri] = field
    
    def needs_update(oef_uri, new_value, value_type):
        if not is_update or not oef_uri:
            return true
        
        current_field = current_oef_values.get(oef_uri, {})
        current_text_value = current_field.get("textValue", "")
        new_text_value = str(new_value) if new_value is not null else ""
        
        return current_text_value != new_text_value
    
    for oef_config in oef_field_mapper_data:
        field_name = oef_config['field_name']
        oef_name = oef_config['oef_name']
        oef_type = oef_config['type']
        can_update = oef_config.get('can_update', True)  # Default to True if not specified
        
        # Skip updates for fields that cannot be updated
        if is_update and not can_update:
            continue
        
        if field_name not in user_data:
            continue
            
        new_value = user_data.get(field_name)
        
        # Handle UKSICK validation for add scenarios - only allow A, B, C
        if not is_update and field_name == 'uksick':
            new_uksick = str(new_value) if new_value else ""
            if new_uksick not in ['A', 'B', 'C'] and new_uksick != "":
                new_value = null
        
        # Handle UKSICK special logic for updates
        if is_update and field_name == 'uksick':
            uksick_oef_uri = processed_oef_data.get(oef_name)
            if uksick_oef_uri:
                current_uksick = current_oef_values.get(uksick_oef_uri, {}).get("textValue", "")
                new_uksick = str(new_value) if new_value else ""
                # Rule 1: If existing A or B, don't update to C (preserve A/B)
                if current_uksick in ['A', 'B'] and new_uksick == 'C':
                    continue  # Skip update
                
                # Rule 2: If not A, B, or C, skip the update entirely
                if new_uksick not in ['A', 'B', 'C']:
                    continue
                
                # Rule 3: If current and new are the same, skip update
                if current_uksick == new_uksick:
                    continue
            
        if not processed_oef_data.get(oef_name):
            if not is_update or needs_update(null, null, oef_type):
                missing_oefs.append(f"{oef_name} OEF is not present in Replicon")
            continue
        
        oef_uri = processed_oef_data[oef_name]
        
        oef_payload = {
            "value": {
                "definition": {
                    "uri": oef_uri,
                    "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": null,
                "fileValue": null,
                "jsonValue": null
            }
        }
        
        if oef_type == 'text':
            if needs_update(oef_uri, new_value, oef_type):
                oef_payload["value"]["textValue"] = str(new_value) if new_value is not null else null
                if is_update:
                    updated_oefs.append(f"{oef_name} updated")
                oefs.append(oef_payload)
    
    if is_update and not oefs:
        missing_oefs = []
        missing_oef_tags = []
        updated_oefs = []
    
    return {
        'payload': oefs,
        'missing_oefs': missing_oefs,
        'missing_oef_tags': missing_oef_tags,
        'updated_oefs': updated_oefs
    }

def get_assign_supervisor_permission_payload(supervisor_permission):
    return {
        "target": {
            "uri": rail.result("get_supervisor_details")["userDetails"]["uri"],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "template": null,
        "modifications": {
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "uri": null,
                                "name": supervisor_permission
                            },
                            "groupAccessFilter": null
                        }
                    ]
                }
            ]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

def get_report_parameters():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_report_details")["uri"],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_exception_logs(dag_run, config):
    """Get comprehensive exception logs from all relevant functions"""
    all_exceptions = []
    
    is_update_scenario = rail.result("get_user_details", {}) is not {}
    
    oef_field_mapper = config.oef_field_mapper_data
    oef_result = get_user_oefs_for_add_update(dag_run, oef_field_mapper, is_update=is_update_scenario)
    missing_oefs = oef_result.get('missing_oefs', [])
    missing_oef_tags = oef_result.get('missing_oef_tags', [])
    
    all_exceptions.extend(missing_oefs)
    all_exceptions.extend(missing_oef_tags)

    if dag_run.conf.get("calculated_location_name") and not dag_run.conf.get("calculated_location_uri"):
        all_exceptions.append(f"Location \'{dag_run.conf.get('calculated_location_name')}\' is not present in Replicon")

    if dag_run.conf.get("calculated_department_name") and not dag_run.conf.get("calculated_department_uri"):
        all_exceptions.append(f"Department \'{dag_run.conf.get('calculated_department_name')}\' is not present in Replicon")

    if dag_run.conf.get("calculated_employee_type") and not dag_run.conf.get("calculated_employee_type_uri"):
        all_exceptions.append(f"Employee type \'{dag_run.conf.get('calculated_employee_type')}\' is not present in Replicon")

    if dag_run.conf.get("calculated_timesheet_template") and not dag_run.conf.get("calculated_timesheet_template_uri"):
        all_exceptions.append(f"Timesheet template \'{dag_run.conf.get('calculated_timesheet_template')}\' is not present in Replicon")
    
    if dag_run.conf.get("calculated_payrule") and not dag_run.conf.get("calculated_payrule_uri"):
        all_exceptions.append(f"Pay rule \'{dag_run.conf.get('calculated_payrule')}\' is not present in Replicon")
    
    if dag_run.conf.get("calculated_holiday_calendar") and not dag_run.conf.get("calculated_holiday_calendar_uri"):
        all_exceptions.append(f"Holiday calendar \'{dag_run.conf.get('calculated_holiday_calendar')}\' is not present in Replicon")
    
    if dag_run.conf.get("title") and not dag_run.conf.get("calculated_project_role_uri"):
        all_exceptions.append(f"Project role for title \'{dag_run.conf.get('title')}\' is not present in Replicon")
    
    if dag_run.conf.get("title") and not dag_run.conf.get("calculated_orgrole_uri"):
        all_exceptions.append(f"Org role for title \'{dag_run.conf.get('title')}\' is not present in Replicon")
    
    # Check permissions - if permissions calculated but some have missing URIs
    calculated_permissions = dag_run.conf.get("calculated_permissions", {})
    if calculated_permissions:
        missing_permissions = []
        for permission_name, permission_uri in calculated_permissions.items():
            if not permission_uri:
                missing_permissions.append(permission_name)
        
        if missing_permissions:
            all_exceptions.append(f'Permissions not present in Replicon: {", ".join(missing_permissions)}')
    
    if dag_run.conf.get("supervisor") and not rail.result("get_supervisor_details") and dag_run.conf.get("supervisor") not in custom_methods.get_all_user_login_names_from_feed(dag_run):
        all_exceptions.append(f'Supervisor "{dag_run.conf.get("supervisor")}" is not available in Replicon')
    if dag_run.conf.get("employee_id") == dag_run.conf.get("supervisor"):
        all_exceptions.append("Supervisor ID cannot be the same as employee ID")
    
    # Resource pool assignment exceptions
    if dag_run.conf.get("calculated_orgrole_code"):
        resource_pool_result = rail.result("get_resource_pool_from_replicon")
        if not resource_pool_result or not resource_pool_result.get("uri"):
            all_exceptions.append(f'Resource pool for org role code "{dag_run.conf.get("calculated_orgrole_code")}" is not present in Replicon')
    
    # Time off types exceptions - check if URIs are missing (timeoff types not present in Replicon)
    calculated_time_off_types = dag_run.conf.get("calculated_time_off_types", {})
    if calculated_time_off_types:
        missing_timeoffs = []
        for timeoff_name, timeoff_config in calculated_time_off_types.items():
            timeoff_uri = timeoff_config.get("uri") if isinstance(timeoff_config, dict) else timeoff_config
            if not timeoff_uri:
                missing_timeoffs.append(timeoff_name)
        
        if missing_timeoffs:
            all_exceptions.append(f'Time off types not present in Replicon: {", ".join(missing_timeoffs)}')
    
    # Activities exceptions - check if URIs are missing (activities not present in Replicon)
    calculated_activities = dag_run.conf.get("calculated_activities", {})
    if calculated_activities:
        missing_activities = []
        for activity_name, activity_uri in calculated_activities.items():
            if not activity_uri:
                missing_activities.append(activity_name)
        
        if missing_activities:
            all_exceptions.append(f'Activities not present in Replicon: {", ".join(missing_activities)}')
    
    # Supervisor permission exceptions
    calculated_supervisor_permission = dag_run.conf.get("calculated_supervisor_permission", "")
    if dag_run.conf.get("supervisor") and not calculated_supervisor_permission:
        all_exceptions.append("Supervisor permission could not be resolved from permission sets")
    
    # Timesheet period exceptions
    if dag_run.conf.get("calculated_timesheet_period") and not dag_run.conf.get("calculated_timesheet_period_uri"):
        all_exceptions.append(f"Timesheet period \'{dag_run.conf.get('calculated_timesheet_period')}\' is not present in Replicon")
    
    # Schedule type exceptions - check for office schedule URI if Office Schedule type
    if dag_run.conf.get("calculated_schedule_type_uri") and "office-schedule" in dag_run.conf.get("calculated_schedule_type_uri"):
        if dag_run.conf.get("calculated_schedule_name") and not dag_run.conf.get("calculated_office_schedule_uri"):
            all_exceptions.append(f"Office schedule \'{dag_run.conf.get('calculated_schedule_name')}\' is not present in Replicon")
    
    return all_exceptions

def get_user_holiday_cal_payload(dag_run, YMD_DATE_FORMAT):
    effective_date = rail.parse_date(dag_run.conf["current_date"], YMD_DATE_FORMAT)
    return {
        "target": {
            "uri": rail.result("get_user_details")["userDetails"]['uri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "dateRange": {
            "startDate": effective_date,
            "endDate": effective_date,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }

def get_resource_pools_payload(dag_run):
    return {
    	"page": "1",
    	"pageSize": "10000",
    	"searchParam": {
    		"statusOptionUri": "urn:replicon:resource-pool-status-option:include-all-resource-pool",
    		"textSearch": {
    			"queryText": dag_run.conf.get("calculated_orgrole_code"),
    			"searchInName": "true",
    			"searchInDescription": "false",
    			"searchInCode": "false"
    		}
    	}
    }

def get_user_assigned_resource_pools_payload(dag_run):
    return {
        "page": 1,
        "pageSize": 10000,
        "user": {
            "uri": rail.result("get_user_details")["userDetails"]['uri']
        },
        "searchParam": {
            "statusOptionUri": "urn:replicon:resource-pool-status-option:include-all-resource-pool",
            "textSearch": {
            	"queryText": dag_run.conf.get("calculated_orgrole_code"),
            	"searchInName": "true",
            	"searchInDescription": "false",
            	"searchInCode": "false"
            }
        }
    }

def get_assign_resource_pool_payload(user_uri):
    return {
        "user": {
            "uri": user_uri
        },
        "resourcePool": {
            "uri": rail.result("get_resource_pool_from_replicon")["uri"] or rail.result("create_resource_pool_in_replicon")["uri"]
        },
        "resourcePoolUserAssignmentOptionUri": "urn:replicon:user-resource-pool-assignment-option:assign"
    }

def get_updated_logs(dag_run, config):
    """Get all update logs from the various helper functions"""
    all_updates = []
    
    # Get basic details updates
    basic_details_result = get_basic_user_details_update(dag_run, config.YMD_DATE_FORMAT, config.REP_DATE_FORMAT)
    basic_details_logs = basic_details_result.get("basic_details_logs", [])
    all_updates.extend(basic_details_logs)
    
    # Get OEF updates
    oef_result = get_user_oefs_for_add_update(dag_run, config.oef_field_mapper_data, is_update=true)
    updated_oefs = oef_result.get('updated_oefs', [])
    all_updates.extend(updated_oefs)
    
    
    # Check for group membership updates
    if get_updated_location(dag_run):
        all_updates.append("Location updated")
    
    if get_updated_department(dag_run):
        all_updates.append("Department updated")
    
    if get_updated_employeetype(dag_run):
        all_updates.append("Employee type updated")
    
    if get_updated_servicecenter(dag_run):
        all_updates.append("Service center updated")
    
    if get_updated_supervisor(dag_run) or (not rail.result("get_supervisor_details") and custom_methods.get_all_user_login_names_from_feed(dag_run)):
        all_updates.append("Supervisor updated")
    
    if get_updated_activities(dag_run):
        all_updates.append("Activities updated")
    
    if get_updated_holiday_calendar(dag_run):
        all_updates.append("Holiday calendar updated")
    
    if get_updated_timesheet_template(dag_run):
        all_updates.append("Timesheet template updated")
    
    if get_updated_permissions(dag_run, config.defaults_mapper_data):
        all_updates.append("Permissions updated")
    
    timeoff_changes = get_updated_timeoff_types(dag_run)
    if timeoff_changes.get("new_timeoff_types"):
        all_updates.append("Time off types updated")
    
    if get_updated_project_role(dag_run):
        all_updates.append("Project role updated")
    
    if get_updated_payrule(dag_run):
        all_updates.append("Pay rule updated")
    
    if get_updated_schedule_type(dag_run):
        all_updates.append("Schedule type updated")
    
    if get_updated_timesheet_period(dag_run):
        all_updates.append("Timesheet period updated")
    
    if get_updated_work_week_start_day(dag_run):
        all_updates.append("Work week start day updated")
    
    if get_updated_timesheet_approval_path(dag_run):
        all_updates.append("Timesheet approval path updated")
    
    if get_updated_time_entry_approval_path(dag_run):
        all_updates.append("Time entry approval path updated")
    
    if get_updated_time_off_approval_path(dag_run):
        all_updates.append("Time off approval path updated")
    
    if get_updated_timeoff_template(dag_run):
        all_updates.append("Time off template updated")

    if rail.result("assign_resource_pool_to_user"):
        all_updates.append("Resource pool updated")
    
    return all_updates

def get_action_type(dag_run, config):
    """Determine the action type based on what is being updated"""
    # Check if this is a termination (end date in past)
    if dag_run.conf.get("end_date"):
        end_date = datetime.strptime(dag_run.conf["end_date"], config.REP_DATE_FORMAT)
        current_date = pendulum.now(config.time_zone).date()
        
        if end_date.date() <= current_date:
            return "Termination"
    
    # Check if this is a rehire (new start date after previous end date)
    user_details_result = rail.result("get_user_details", {})
    if user_details_result:
        current_user = user_details_result.get("userDetails", {})
        employment_date_range = current_user.get("employmentDateRange", {})
        existing_end_date = employment_date_range.get("endDate")
        
        if existing_end_date and dag_run.conf.get("start_date"):
            existing_end = datetime(existing_end_date["year"], existing_end_date["month"], existing_end_date["day"])
            new_start = datetime.strptime(dag_run.conf["start_date"], config.REP_DATE_FORMAT)
            
            if new_start > existing_end:
                return "Rehire"
    
    return "Update"

def get_updated_project_role(dag_run):
    """Check if project role needs update"""
    if not dag_run.conf.get("calculated_project_role_uri"):
        return null
    
    # get_user_assigned_role_from_replicon returns array of schedule items directly
    existing_project_roles = rail.result("get_user_assigned_role_from_replicon", [])
    
    current_project_role_schedule = custom_methods.get_currently_effective_item(existing_project_roles, dag_run.conf["current_date"]) if existing_project_roles else null
    
    # Find primary project role from the current schedule
    current_project_role_uri = null
    if current_project_role_schedule and current_project_role_schedule.get("projectRoles"):
        for role in current_project_role_schedule.get("projectRoles", []):
            if role.get("isPrimary"):
                current_project_role_uri = role.get("projectRole", {}).get("uri")
                break
    
    if current_project_role_uri != dag_run.conf["calculated_project_role_uri"]:
        return {
            "new_uri": dag_run.conf["calculated_project_role_uri"],
            "has_existing": current_project_role_uri is not null
        }
    
    return null

def get_updated_payrule(dag_run):
    """Check if pay rule needs update"""
    if not dag_run.conf.get("calculated_payrule_uri"):
        return null
    
    user_details_result = rail.result("get_user_details", {})
    existing_payrule = user_details_result.get("payRuleScriptSchedule", []) if user_details_result else []
    
    current_payrule = custom_methods.get_currently_effective_item(existing_payrule, dag_run.conf["current_date"]) if existing_payrule else null
    current_payrule_uri = current_payrule.get("payRuleScript", {}).get("uri") if current_payrule else null
    
    if current_payrule_uri != dag_run.conf["calculated_payrule_uri"]:
        return {
            "new_uri": dag_run.conf["calculated_payrule_uri"],
            "has_existing": current_payrule_uri is not null
        }
    
    return null

def get_updated_schedule_type(dag_run):
    """Check if schedule type needs update"""
    if not dag_run.conf.get("calculated_schedule_type_uri"):
        return null
    
    user_details_result = rail.result("get_user_details", {})
    existing_schedule = user_details_result.get("schedulePolicies", []) if user_details_result else []
    
    current_schedule = custom_methods.get_currently_effective_item(existing_schedule, dag_run.conf["current_date"]) if existing_schedule else null
    current_schedule_type = current_schedule.get("scheduleTypeUri") if current_schedule else null
    current_office_schedule = current_schedule.get("officeSchedule", {}).get("uri") if (current_schedule and current_schedule.get("officeSchedule")) else null
    
    new_schedule_type = dag_run.conf["calculated_schedule_type_uri"]
    new_office_schedule = dag_run.conf.get("calculated_office_schedule_uri")
    
    if (current_schedule_type != new_schedule_type or 
        current_office_schedule != new_office_schedule):
        return {
            "schedule_type_uri": new_schedule_type,
            "office_schedule_uri": new_office_schedule,
            "has_existing": current_schedule_type is not null
        }
    
    return null

def get_updated_timesheet_period(dag_run):
    """Check if timesheet period needs update"""
    if not dag_run.conf.get("calculated_timesheet_period_uri"):
        return null
    
    user_details_result = rail.result("get_user_details", {})
    existing_period = user_details_result.get("timesheetPeriodSchedule", []) if user_details_result else []
    
    current_period = custom_methods.get_currently_effective_item(existing_period, dag_run.conf["current_date"]) if existing_period else null
    current_period_uri = current_period.get("timesheetPeriod", {}).get("uri") if current_period else null
    
    if current_period_uri != dag_run.conf["calculated_timesheet_period_uri"]:
        return {
            "new_uri": dag_run.conf["calculated_timesheet_period_uri"],
            "has_existing": current_period_uri is not null
        }
    
    return null

def get_updated_work_week_start_day(dag_run):
    """Check if work week start day needs update"""
    if not dag_run.conf.get("calculated_work_week_start_day"):
        return null
    
    weekday = dag_run.conf["calculated_work_week_start_day"].split('-')[0]
    new_work_week_uri = f"urn:replicon:day-of-week:{weekday.lower()}"
    
    user_details_result = rail.result("get_user_details", {})
    if not user_details_result:
        return new_work_week_uri
    
    current_user = user_details_result.get("userDetails", {})
    current_work_week = current_user.get("workWeekStartDay", {}).get("uri")
    
    if current_work_week != new_work_week_uri:
        return new_work_week_uri
    
    return null

def get_updated_timesheet_approval_path(dag_run):
    """Check if timesheet approval path needs update"""
    if not dag_run.conf.get("calculated_timesheet_approval_path"):
        return null
    
    user_details_result = rail.result("get_user_details", {})
    current_user = user_details_result if user_details_result else {}
    
    current_timesheet_approval = current_user.get("timesheetApprovalPath", {}).get("displayText")
    new_timesheet_approval = dag_run.conf["calculated_timesheet_approval_path"]
    
    if current_timesheet_approval != new_timesheet_approval:
        return {
            "value": {
                "uri": null,
                "name": new_timesheet_approval
            }
        }
    
    return null

def get_updated_time_entry_approval_path(dag_run):
    """Check if time entry approval path needs update - only when location/department/employee category changes"""
    if not dag_run.conf.get("calculated_time_entry_approval_path"):
        return null
    
    # Only update if location, department, or employee category changes
    if not (get_updated_location(dag_run) or get_updated_department(dag_run) or get_updated_employeetype(dag_run)):
        return null
    
    # Since timeEntryApprovalPath doesn't exist in get_user_details, always update when org structure changes
    return {
        "value": {
            "uri": null,
            "name": dag_run.conf["calculated_time_entry_approval_path"]
        }
    }

def get_updated_time_off_approval_path(dag_run):
    """Check if time off approval path needs update"""
    if not dag_run.conf.get("calculated_time_off_approval"):
        return null
    
    user_details_result = rail.result("get_user_details", {})
    current_user = user_details_result if user_details_result else {}
    
    current_timeoff_approval = current_user.get("timeOffApprovalPath", {}).get("displayText")
    new_timeoff_approval = dag_run.conf["calculated_time_off_approval"]
    
    if current_timeoff_approval != new_timeoff_approval:
        return {
            "value": {
                "uri": null,
                "name": new_timeoff_approval
            }
        }
    
    return null

def get_updated_timeoff_template(dag_run):
    """Check if time off template needs update"""
    if not dag_run.conf.get("calculated_time_off_template"):
        return null
    
    user_details_result = rail.result("get_user_details", {})
    if not user_details_result:
        return dag_run.conf["calculated_time_off_template"]
    
    current_timeoff_template = user_details_result.get("timeOffTemplate", {}).get("displayText") if user_details_result.get("timeOffTemplate") else null
    new_timeoff_template = dag_run.conf["calculated_time_off_template"]
    
    if current_timeoff_template != new_timeoff_template:
        return new_timeoff_template
    
    return null

def check_fte_or_schedule_or_location_or_level_changes(dag_run):
    """Check if FTE, scheduled hours, location, level, or timeoff types have changed requiring policy updates
    
    Returns:
        dict: {
            'any_changes': bool,
            'is_fte_changed': bool,
            'is_scheduled_hours_changed': bool,
            'is_location_changed': bool,
            'is_level_changed': bool,
            'is_timeoff_changed': bool
        }
    """
    changes = {
        'any_changes': False,
        'is_fte_changed': False,
        'is_scheduled_hours_changed': False,
        'is_location_changed': False,
        'is_level_changed': False,
        'is_timeoff_changed': False
    }
    
    # First check if calculated_time_off_types is present
    if not dag_run.conf.get("calculated_time_off_types"):
        return changes
    
    # Get current user data
    user_details_result = rail.result("get_user_details", {})
    if not user_details_result:
        return changes
    
    # Check for location URI changes
    current_group_membership = rail.result("get_current_group_membership", {})
    current_location_uri = current_group_membership.get("existinglocationuri") if current_group_membership else null
    new_location_uri = dag_run.conf.get("calculated_location_uri")
    
    if new_location_uri and str(new_location_uri) != str(current_location_uri):
        changes['is_location_changed'] = True
        changes['any_changes'] = True
    
    # Get current values from extension fields in single loop
    current_level = null
    current_fte = null
    current_scheduled_hours = null
    extension_fields = user_details_result.get("userDetails", {}).get("extensionFieldValues", [])
    
    for field in extension_fields:
        field_display_text = field.get("definition", {}).get("displayText")
        if field_display_text == "Level":
            current_level = field.get("textValue")
        elif field_display_text == "FTE":
            current_fte = field.get("textValue")
        elif field_display_text == "Scheduled Hours":
            current_scheduled_hours = field.get("textValue")
    
    # Compare with new values from DAG conf
    new_level = dag_run.conf.get("level")
    new_fte = dag_run.conf.get("fte")
    new_scheduled_hours = dag_run.conf.get("scheduled_hours")
    
    # Check if Level has changed
    if new_level and str(new_level) != str(current_level):
        changes['is_level_changed'] = True
        changes['any_changes'] = True
    
    # Check if FTE has changed
    if new_fte and str(new_fte) != str(current_fte):
        changes['is_fte_changed'] = True
        changes['any_changes'] = True
        
    # Check if scheduled hours have changed
    if new_scheduled_hours and str(new_scheduled_hours) != str(current_scheduled_hours):
        changes['is_scheduled_hours_changed'] = True
        changes['any_changes'] = True
    
    # Check for timeoff type changes
    timeoff_changes = get_updated_timeoff_types(dag_run)
    if timeoff_changes.get("new_timeoff_types"):
        changes['is_new_timeoffs_added'] = True
        changes['any_changes'] = True

    # FIXED LOGIC: Only create new policy lines when FTE/Schedule changed AND overlapping types exist
    fte_or_schedule_changed = ((new_fte and str(new_fte) != str(current_fte)) or 
                              (new_scheduled_hours and str(new_scheduled_hours) != str(current_scheduled_hours)))
    
    if fte_or_schedule_changed and timeoff_changes.get("overlapping_timeoff_types"):
        changes['is_new_policy_line_to_be_added'] = True
        changes['any_changes'] = True
    
    return changes

def get_supervisor_assignment_payload(dag_run, YMD_DATE_FORMAT):
    """Get payload for supervisor assignment in supervisor assignment child DAG"""
    
    supervisor_uri = rail.result("get_supervisor_details")["userDetails"]["uri"]
    current_date = rail.parse_date(dag_run.conf["current_date"], YMD_DATE_FORMAT)
    
    # Check if user has existing supervisor
    supervisor_assignment = rail.result("get_supervisor_assignment_details", "supervisor") if rail.result("get_supervisor_assignment_details") else null
    existing_supervisor = supervisor_assignment.get("user", {}).get("uri") if supervisor_assignment else null
    
    return {
        "target": {
            "uri": dag_run.conf.get("user_uri"),
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "template": null,
        "modifications": {
            "supervisorSchedule": [
                {
                    "dateRange": {
                        "startDate": current_date if existing_supervisor else null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": supervisor_uri,
                        "loginName": null,
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                }
            ]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

def put_timeoff_assignment_payload(dag_run):
    all_calculated_types = dag_run.conf.get("calculated_time_off_types", {})
    return {
        'userUri': rail.result("get_user_details")["userDetails"]['uri'],
        'timeOffTypeUris': [timeoff_type["uri"] for timeoff_type in all_calculated_types.values()]
    }

def get_disable_user_and_update_loginname_payload(dag_run):
    return {
        "target": {
            "uri": dag_run.conf["useruri"]
        },
        "template": null,
        "modifications": {
            "loginName": {
                "value": f'{dag_run.conf["loginname"]}_{dag_run.conf["userenddate"]}'
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": False
                    }
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }
