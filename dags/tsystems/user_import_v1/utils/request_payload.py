import json
import uuid
import rail
from datetime import datetime
from tsystems.user_import_v1.utils import custom_methods

null = None
true = True
false = False

def get_user_details_payload(api_keys_mapper, data_source, data_source_stage, changed_since, filter_query):
    return json.dumps({
        "dataSource": data_source,
        "dataSourceStage": data_source_stage,
        "changedSince": changed_since,
        "filter": filter_query,
        "attributes": list(api_keys_mapper.values())
    })

def get_put_oef_tags_payload(oef_uri, oef_tag_details, oef_field_name, new_oef_tags):
    return {
        "objectExtensionTagDefinition": {
            "uri": oef_uri,
            "name": null
        },
        "objectExtensionTags": [
            # Existing tags
            *[{
                "target": {
                    "uri": tag["uri"],
                    "slug": null,
                    "tagName": null
                },
                "name": tag["oef_tag"],
                "code": tag.get("code"),
                "description": tag.get("description"),
                "isEnabled": tag.get("is_enabled", true)
            } for tag in oef_tag_details],
            # New tags
            *[{
                "target": {
                    "uri": null,
                    "slug": null,
                    "tagName": null
                },
                "name": item[oef_field_name],
                "code": null,
                "description": null,
                "isEnabled": true
            } for item in rail.load_all_records(new_oef_tags)]
        ]
    }

def get_user_creation_payload(dag_run, config):
    """
    Build payload for creating a new user in T-Systems format
    """
    user_data = dag_run.conf

    # Parse dates
    start_date = rail.parse_date(user_data.get('startdate'), config.REP_DATE_FORMAT) if user_data.get('startdate') else null
    end_date = rail.parse_date(user_data.get('enddate'), config.REP_DATE_FORMAT) if user_data.get('enddate') else null
    
    # Determine login status - check if end date is in past
    login_enabled = user_data.get('calculated_login_status', 'Non Active') == 'Active'
    
    # If end date is in the past, disable the user regardless of calculated_login_status
    if user_data.get('enddate'):
        import pendulum
        end_date_obj = datetime.strptime(user_data.get('enddate'), config.REP_DATE_FORMAT)
        current_date_obj = pendulum.now(config.time_zone).date()
        
        if end_date_obj.date() <= current_date_obj:
            login_enabled = false

    payload = {
        "target": null,
        "template": null,
        "modifications": {
            "firstName": {
                "value": user_data.get('firstname') or config.defaults_mapper_data.get('first_name', 'unknown')
            },
            "lastName": {
                "value": user_data.get('lastname') or config.defaults_mapper_data.get('last_name', 'unknown')
            },
            "loginName": {
                "value": user_data.get('email')  # Using email as login name per T-Systems spec
            },
            "displayName": {
                "value": user_data.get('calculated_display_name')
            },
            "emailAddress": {
                "value": user_data.get('email')
            },
            "employeeId": {
                "value": user_data.get('employeeid')
            },
            "employmentDateRange": {
                "value": {
                    "startDate": start_date,
                    "endDate": end_date,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            } if start_date or end_date else null,
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": login_enabled
                    },
                    "forcePasswordChange": null,
                    "ssoName": {
                        "value": user_data.get('email')  # SSO name is email per T-Systems spec
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
                    "name": config.defaults_mapper_data.get('timesheet_approval_path')
                }
            },
            "timeEntryApprovalPath": null,
            "workAuthorizationApprovalPath": null,
            "timeoffApprovalPath": {
                "value": {
                    "uri": null,
                    "name": config.defaults_mapper_data.get('timeoff_approval_path')
                }
            },
            "timeOffBalancePayoutApprovalPath": null,
            "defaultActivity": null,
            "expenseApprovalPath": null,
            "timeZone": {
                "value": {
                    "uri": user_data.get('calculated_time_zone_uri'),
                    "name": null
                }
            } if user_data.get('calculated_time_zone_uri') else null,
            "workWeekStartDay": {
                "value": {
                    "uri": "urn:replicon:day-of-week:monday"  # Monday to Sunday per T-Systems spec
                }
            },
            "defaultBillingRate": null,
            "userPreferences": null,
            "formattings": null,
            "notificationPreferences": null,
            "timesheetTemplate": null,
            "timeoffTemplate": {
                "value": {
                    "uri": null,
                    "name": config.defaults_mapper_data.get('timeoff_template')
                }
            },
            "timeOffCalendarVisibility": null,
            "expenseTemplate": null,
            "workAuthorizationTemplate": null,
            "punchEntryPolicy": null,
            "holidayCalendar": null,
            "extensionFields": get_user_oefs_for_add_update(dag_run, config.oef_field_mapper_data, is_update=false)["payload"],
            "customFields": get_user_custom_fields_for_add_update(dag_run, config.custom_field_mapper_data, config.REP_DATE_FORMAT, is_update=false)["payload"],
            "products": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "name": license
                        }
                    ]
                } for license in config.defaults_mapper_data.get('licenses', [])
            ],
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
                                "uri": user_data.get("calculated_permissions", {}).get(permission),
                                "name": permission
                            },
                            "groupAccessFilter": null
                        }
                        for permission in user_data.get("calculated_permissions", {}).keys()
                    ]
                }
            ] if user_data.get("calculated_permissions") else [],
            "bankedTimePolicies": [],
            "timeOffTypes": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "timeOffType": {
                                "uri": user_data.get("calculated_time_off_types", {}).get(timeoff_type),
                                "name": timeoff_type
                            },
                            "isTimeOffAllowedAgainstThisTimeOffType": true,
                            "applyDefaultTimeOffTypePolicy": true,
                            "defaultTimeOffTypePolicyEffectiveDate": start_date,
                            "policySchedule": []
                        }
                        for timeoff_type in user_data.get("calculated_time_off_types", {}).keys()
                    ]
                }
            ] if user_data.get("calculated_time_off_types") else [],
            "locationSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": user_data.get("calculated_orgstructure_uri"),
                        "parentUri": null,
                        "name": null
                    }
                }
            ] if user_data.get("calculated_orgstructure_uri") else [],
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
                        "uri": user_data.get("calculated_department_uri"),
                        "parentUri": null,
                        "name": null
                    }
                }
            ] if user_data.get("calculated_department_uri") else [],
            "departmentGroupSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": user_data.get("calculated_costcenter_uri"),
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    }
                }
            ] if user_data.get("calculated_costcenter_uri") else [],
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
                        "loginName": null,
                        "employeeId": user_data.get('supervisorempid'),
                        "parameterCorrelationId": null
                    }
                }
            ] if rail.result("get_supervisor_details") else [],
            "timesheetPeriodSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "name": config.defaults_mapper_data.get('timesheet_period')
                    }
                }
            ],
            "holidayCalendarSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": user_data.get('holiday_calendar_uri'),
                        "name": null
                    }
                }
            ] if user_data.get('holiday_calendar_uri') else [],
            "scheduleTypeSchedule": [],
            "payRuleSchedule": [],
            "placeSchedule": [],
            "payRateSchedule": [],
            "projectRoleSchedule": [],
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
        "unitOfWorkId": str(uuid.uuid4())
    }
    
    return payload

def get_update_user_req(dag_run, oef_field_mapper, timesheet_template_mapper, employee_type_mapper, permissions_mapper, custom_field_mapper, YMD_DATE_FORMAT, REP_DATE_FORMAT):
    current_date = rail.parse_date(dag_run.conf["current_date"], YMD_DATE_FORMAT)
    
    # Initialize group membership result for reuse across different update sections
    group_membership_result = null
    
    # Track if we have any modifications
    has_modifications = false
    
    # Get basic user details modifications
    basic_details_result = get_basic_user_details_update(dag_run, YMD_DATE_FORMAT, REP_DATE_FORMAT)
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
    employee_type_update = get_updated_employeetype(dag_run, employee_type_mapper)
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
        supervisor_details = rail.result("get_supervisor_assignment_details")
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

    activities_update = get_updated_activities(dag_run, employee_type_mapper)
    if activities_update:
        has_modifications = true

        # Get current activities with their URIs
        user_details_result = rail.result("get_user_details", {})
        current_activities = user_details_result.get("assignedActivities", []) if user_details_result else []
        current_uris = {activity.get("uri") for activity in current_activities if activity.get("uri")}

        # Get new URIs from the update dictionary
        new_uris = set(activities_update.values())

        # Calculate which URIs to remove and add
        uris_to_remove = current_uris - new_uris
        uris_to_add = new_uris - current_uris

        # Build modifications structure
        modifications["activities"] = []

        if uris_to_remove:
            modifications["activities"].append({
                "modificationOptionUri": "urn:replicon:collection-modification-option:remove",
                "items": [{"uri": uri} for uri in uris_to_remove]
            })

        if uris_to_add:
            modifications["activities"].append({
                "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                "items": [{"uri": uri} for uri in uris_to_add]
            })
    
    timezone_update = get_updated_timezone(dag_run)
    if timezone_update:
        has_modifications = true
        modifications["timeZone"] = {
            "value": {
                "uri": timezone_update
            }
        }
    
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
    timesheet_template_update = get_updated_timesheet_template(dag_run, timesheet_template_mapper)
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
    permission_updates = get_updated_permissions(dag_run, permissions_mapper, employee_type_mapper)
    if permission_updates:
        has_modifications = true
        modifications["permissionSets"] = permission_updates
    
    # Check for OEF updates
    oef_result = get_user_oefs_for_add_update(dag_run, oef_field_mapper, is_update=true)
    if oef_result["payload"]:
        has_modifications = true
        modifications["extensionFields"] = oef_result["payload"]
    
    # Check for custom field updates
    custom_field_result = get_user_custom_fields_for_add_update(dag_run, custom_field_mapper, REP_DATE_FORMAT, is_update=true)
    if custom_field_result["payload"]:
        has_modifications = true
        modifications["customFields"] = custom_field_result["payload"]
    
    # Handle time off types - add new timeoff types only
    timeoff_changes = get_updated_timeoff_types(dag_run)
    if timeoff_changes["new_timeoff_types"]:
        has_modifications = true
        assign_items = []
        for uri in timeoff_changes["new_timeoff_types"]:
            assign_items.append({
                "timeOffType": {
                    "uri": uri
                },
                "isTimeOffAllowedAgainstThisTimeOffType": true,
                "applyDefaultTimeOffTypePolicy": true,
                "defaultTimeOffTypePolicyEffectiveDate": current_date,
                "policySchedule": []
            })
        
        modifications["timeOffTypes"] = [{
            "modificationOptionUri": "urn:replicon:collection-modification-option:add",
            "items": assign_items
        }]
    
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
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_basic_user_details_update(dag_run, YMD_DATE_FORMAT, REP_DATE_FORMAT):
    """
    Build basic user details update payload per T-Systems tech spec
    Args:
        dag_run: DAG run object
    Returns: dict with modifications and logs
    """
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
    first_name = dag_run.conf.get("firstname") or "unknown"
    last_name = dag_run.conf.get("lastname") or "unknown"
    display_name = f"{first_name} {last_name}"
    
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

    # Check if loginName needs update (compare email with current loginName)
    sso_name_to_update = null
    if dag_run.conf.get("email") and dag_run.conf["email"] != current_security.get("loginName"):
        modifications["loginName"] = {"value": dag_run.conf["email"]}
        sso_name_to_update = dag_run.conf["email"]
        basic_details_logs.append(f"Login name and SSO name updated")

    # Check for rehire scenario FIRST
    is_rehire = false
    if (not current_security.get("isLoginEnabled") and existing_end_date):
        # Get current Unique ID and Date of Employment
        current_unique_id = rail.find_first_by_attr_and_get_attr(
            current_user.get("extensionFieldValues", []), 
            "definition.displayText", 
            "Unique ID of employment", 
            "textValue"
        )

        # Get current Date of Employment (assuming it's stored as a custom field)
        current_date_of_employment = null
        for field in current_user.get("customFieldValues", []):
            if field.get("customField", {}).get("name") == "Date of Employment":
                date_val = field.get("date")
                if date_val:
                    current_date_of_employment = f"{date_val['day']:02d}.{date_val['month']:02d}.{date_val['year']}"
                break
            
        # Check if BOTH fields have changed
        # Handle null to value scenario explicitly for uniqueid_of_employment
        new_unique_id = dag_run.conf.get("uniqueid_of_employment")
        unique_id_changed = (
            new_unique_id and 
            (current_unique_id is null or 
             str(current_unique_id) != str(new_unique_id))
        )

        # Handle null to value scenario explicitly for date_of_employment  
        new_date_of_employment = dag_run.conf.get("date_of_employment")
        date_of_employment_changed = (
            new_date_of_employment and 
            (current_date_of_employment is null or 
             current_date_of_employment != new_date_of_employment)
        )

        if unique_id_changed and date_of_employment_changed:
            # Date of Employment must be future dated for rehire
            employment_date = datetime.strptime(dag_run.conf["date_of_employment"], REP_DATE_FORMAT)
            if employment_date > current_date:
                is_rehire = true
                # basic_details_logs.append("Rehire detected - both unique ID and date of employment changed with future date")
    
    # Check employment dates
    employment_date_changed = false
    employment_date_value = {}
    
    # Handle start date
    start_date_changed = false
    if dag_run.conf.get("startdate") or is_rehire:
        if is_rehire and dag_run.conf.get("date_of_employment"):
            # Use Date of Employment for rehire
            new_start_date = rail.parse_date(dag_run.conf["date_of_employment"], REP_DATE_FORMAT)
            employment_date_value["startDate"] = new_start_date
            start_date_changed = true
            basic_details_logs.append(f"Start date updated (rehire)")
        elif dag_run.conf.get("startdate"):
            # Simply compare the date strings after normalizing format
            new_start_date = rail.parse_date(dag_run.conf["startdate"], REP_DATE_FORMAT)
            # Convert to comparable format
            new_start_str = f'{new_start_date["year"]}/{new_start_date["month"]:02d}/{new_start_date["day"]:02d}'
            
            if not existing_start_date or new_start_str != existing_start_date:
                employment_date_value["startDate"] = new_start_date
                start_date_changed = true
                basic_details_logs.append(f"Start date updated")
    
    # Handle end date
    end_date_changed = false
    if is_rehire:
        # Remove end date for rehire
        employment_date_value["endDate"] = null
        end_date_changed = true
        basic_details_logs.append("End date removed (rehire)")
    elif dag_run.conf.get("enddate"):
        # Validate termination: end date must be > start date
        end_date = datetime.strptime(dag_run.conf["enddate"], REP_DATE_FORMAT)
        start_date_str = dag_run.conf.get("startdate", existing_start_date)
        if start_date_str:
            start_date = datetime.strptime(start_date_str, REP_DATE_FORMAT if dag_run.conf.get("startdate") else YMD_DATE_FORMAT)

            if end_date > start_date:
                new_end_date = rail.parse_date(dag_run.conf["enddate"], REP_DATE_FORMAT)
                # Convert to comparable format
                new_end_str = f'{new_end_date["year"]}/{new_end_date["month"]:02d}/{new_end_date["day"]:02d}'

                if not existing_end_date or new_end_str != existing_end_date:
                    employment_date_value["endDate"] = new_end_date
                    end_date_changed = true
                    basic_details_logs.append(f"End date updated")
            else:
                basic_details_logs.append(f"End date '{dag_run.conf['enddate']}' not applied - must be after start date")
    elif not dag_run.conf.get("enddate"):
        # Clear end date if enddate is not provided (key missing or blank/null) and user has existing end date
        if existing_end_date:
            employment_date_value["endDate"] = null
            end_date_changed = true
            basic_details_logs.append("End date cleared")
    
    # Only set employment_date_changed if either date actually changed
    employment_date_changed = start_date_changed or end_date_changed
    
    # If any date changed, we need to include both dates to maintain the complete range
    if employment_date_changed:
        # Add unchanged dates to maintain complete range structure
        if not start_date_changed:
            employment_date_value["startDate"] = rail.parse_date(existing_start_date, YMD_DATE_FORMAT) if existing_start_date else null
        if not end_date_changed:
            employment_date_value["endDate"] = rail.parse_date(existing_end_date, YMD_DATE_FORMAT) if existing_end_date else null
    
    if employment_date_changed:
        employment_date_value["relativeDateRangeUri"] = null
        employment_date_value["relativeDateRangeAsOfDate"] = null
        modifications["employmentDateRange"] = {"value": employment_date_value}
    
    # Get login status from master DAG calculation
    should_be_active = dag_run.conf.get("calculated_login_status") == "Active"
    can_update_login_status = rail.find_first_by_attr_and_get_attr(current_user.get("extensionFieldValues"), "definition.displayText", "Manually Updated", "tag.displayText") != "Yes"

    # Track if user is being disabled
    is_disabled = false

    # Track if end date is being cleared in this update
    end_date_being_cleared = end_date_changed and employment_date_value.get("endDate") is null

    # TERMINATION LOGIC - Handle end date updates and login status for terminated users
    if dag_run.conf.get("enddate") and not is_rehire:
        end_date = datetime.strptime(dag_run.conf["enddate"], REP_DATE_FORMAT)
        start_date_str = dag_run.conf.get("startdate", existing_start_date)
        
        # Only process termination if end_date > start_date (per tech spec)
        if start_date_str:
            start_date = datetime.strptime(start_date_str, REP_DATE_FORMAT if dag_run.conf.get("startdate") else YMD_DATE_FORMAT)
            
            if end_date > start_date:
                # Valid termination - proceed with login status logic
                if end_date <= current_date:
                    # Past termination - disable unconditionally (ignore Login Status Mapper)
                    if current_security.get("isLoginEnabled"):
                        is_disabled = true
                        modifications["securitySettings"] = {
                            "value": {"loginEnabled": {"value": false}}
                        }
                        basic_details_logs.append(f"Login status disabled (termination - end date '{dag_run.conf['enddate']}' in past)")
            else:
                # Invalid termination date - skip termination logic
                basic_details_logs.append(f"End date '{dag_run.conf['enddate']}' not greater than start date")
    
    # REHIRE LOGIC - Handle rehire scenarios
    elif is_rehire:
        # Check Login Status Mapper for rehire activation (as per tech spec line 644)
        if should_be_active:
            modifications["securitySettings"] = {
                "value": {"loginEnabled": {"value": true}}
            }
            basic_details_logs.append("Login status enabled (rehire)")
        else:
            basic_details_logs.append("Rehire detected but login mapper prevents activation")
    
    # LOGIN STATUS MAPPER LOGIC - Handle normal login status updates
    elif can_update_login_status and not should_be_active and current_security.get("isLoginEnabled"):
        # Disable based on mapper
        is_disabled = true
        modifications["securitySettings"] = {
            "value": {"loginEnabled": {"value": false}}
        }
        basic_details_logs.append("Login status disabled")
    
    elif can_update_login_status and should_be_active and not current_security.get("isLoginEnabled"):
        # Enable based on mapper - check if end date prevents activation
        # Skip this check if end date is being cleared in this update
        end_date_prevents_activation = false
        if existing_end_date and not end_date_being_cleared:
            existing_end = datetime.strptime(existing_end_date, YMD_DATE_FORMAT)
            if existing_end <= current_date:
                end_date_prevents_activation = true
                basic_details_logs.append(f"Login not enabled - existing end date '{existing_end_date}' is in the past")
        
        if not end_date_prevents_activation:
            # Enable based on mapper (Login Status Mapper takes precedence for non-terminated users)
            modifications["securitySettings"] = {
                "value": {"loginEnabled": {"value": true}}
            }
            basic_details_logs.append("Login status enabled")

    # Merge ssoName into securitySettings if needed
    if sso_name_to_update:
        if "securitySettings" in modifications:
            modifications["securitySettings"]["value"]["ssoName"] = {"value": sso_name_to_update}
        else:
            modifications["securitySettings"] = {
                "value": {"ssoName": {"value": sso_name_to_update}}
            }

    return {
        "modifications": modifications,
        "basic_details_logs": basic_details_logs,
        "is_rehire": is_rehire,
        "is_disabled": is_disabled
    }


def get_updated_location(dag_run):
    return dag_run.conf["calculated_orgstructure_uri"] if (dag_run.conf["calculated_orgstructure_uri"] and
        dag_run.conf["calculated_orgstructure_uri"] != rail.result("get_current_group_membership")["existinglocationuri"]) else null


def get_updated_department(dag_run):
    return dag_run.conf["calculated_costcenter_uri"] if (dag_run.conf["calculated_costcenter_uri"] and
        dag_run.conf["calculated_costcenter_uri"] != rail.result("get_current_group_membership")["existingdepartmenturi"]) else null


def get_updated_employeetype(dag_run, employee_type_mapper):
    """
    Check if employee type should be updated, considering exception rules from mapper
    """
    # First check if there's a new calculated employee type
    if not dag_run.conf.get("calculated_employee_type_uri"):
        return null
    
    # Get current employee type details
    current_group_membership = rail.result("get_current_group_membership")
    current_employee_type_uri = current_group_membership.get("existingemployeetypeuri") if current_group_membership else null
    current_employee_type_name = current_group_membership.get("existingemployeetypename") if current_group_membership else null
    
    # If URIs are the same, no update needed
    if dag_run.conf["calculated_employee_type_uri"] == current_employee_type_uri:
        return null
    
    # Check if current employee type should be preserved based on exception rules
    if (employee_type_mapper and 
        'exceptions' in employee_type_mapper and 
        current_employee_type_name):
        
        # Get user's current attributes from Replicon (not from incoming data)
        user_attrs = get_user_attributes_from_replicon(dag_run)
        
        # Check exception rules using shared function
        for exception in employee_type_mapper['exceptions']:
            if check_exception_rules(exception, current_employee_type_name, 'employee_type', user_attrs):
                # Exception matched - preserve current employee type
                return null
    
    # If we reach here, no exception applies - update the employee type
    return dag_run.conf["calculated_employee_type_uri"]

def get_updated_timesheet_template(dag_run, timesheet_template_mapper):
    """
    Check if timesheet template should be updated, considering exception rules from mapper
    """
    # First check if there's a new calculated timesheet template
    if not dag_run.conf.get("calculated_timesheet_template_uri"):
        return null
    
    # Get current timesheet template details (at root level, not under userDetails)
    user_details_result = rail.result("get_user_details", {})
    current_timesheet_template = user_details_result.get("timesheetTemplate", {}) if user_details_result else {}
    current_timesheet_template_uri = current_timesheet_template.get("uri") if current_timesheet_template else null
    current_timesheet_template_name = current_timesheet_template.get("name") if current_timesheet_template else null
    
    # If URIs are the same, no update needed  
    if dag_run.conf["calculated_timesheet_template_uri"] == current_timesheet_template_uri:
        return null
    
    # Check if current timesheet template should be preserved based on exception rules
    if (timesheet_template_mapper and 
        'exceptions' in timesheet_template_mapper and 
        current_timesheet_template_name):
        
        # Get user's current attributes from Replicon (not from incoming data)
        user_attrs = get_user_attributes_from_replicon(dag_run)
        
        # Check exception rules using shared function
        for exception in timesheet_template_mapper['exceptions']:
            if check_exception_rules(exception, current_timesheet_template_name, 'timesheet_template', user_attrs):
                # Exception matched - preserve current timesheet template
                return null
    
    # If we reach here, no exception applies - update the timesheet template
    return dag_run.conf["calculated_timesheet_template_uri"]


def get_updated_servicecenter(dag_run):
    return dag_run.conf["calculated_department_uri"] if (dag_run.conf["calculated_department_uri"] and
        dag_run.conf["calculated_department_uri"] != rail.result("get_current_group_membership")["existingservicecenteruri"]) else null


def get_updated_supervisor(dag_run):
    supervisor_id = dag_run.conf.get("supervisorempid")
    employee_id = dag_run.conf.get("employeeid")
    
    if not supervisor_id or supervisor_id == employee_id:
        return null
    
    # Get supervisor URI if supervisor exists in system
    supervisor_details_result = rail.result("get_supervisor_details")
    if not supervisor_details_result:
        return null
        
    supervisor_uri = supervisor_details_result["userDetails"]["uri"]
    
    # Check current supervisor assignment
    supervisor_assignment = rail.result("get_supervisor_assignment_details")
    current_supervisor_uri = supervisor_assignment.get("user", {}).get("uri") if supervisor_assignment else null
    
    return supervisor_uri if supervisor_uri != current_supervisor_uri else null

def get_updated_activities(dag_run, employee_type_mapper):
    # Activities should update if EITHER location OR employee type changes (not both required)
    return dag_run.conf.get("calculated_activities") if (get_updated_location(dag_run) or get_updated_employeetype(dag_run, employee_type_mapper)) else null

def get_updated_timezone(dag_run):
    current_country_of_employment = rail.find_first_by_attr_and_get_attr(rail.result("get_user_details")["userDetails"]["extensionFieldValues"],
        "definition.displayText", "Country of Employment", "tag.displayText")
    return dag_run.conf["calculated_time_zone_uri"] if current_country_of_employment != dag_run.conf["country_of_employment"] else null

def get_updated_holiday_calendar(dag_run):
    return (dag_run.conf["holiday_calendar_uri"] if dag_run.conf["holiday_calendar_uri"]
        and dag_run.conf["holiday_calendar_uri"] != (rail.result("get_user_holiday_calendar")["uri"]
            if rail.result("get_user_holiday_calendar") else null)
    		    else null)

def get_updated_timeoff_types(dag_run):
    """
    Compare current user timeoff types with calculated ones and return URIs for changes.
    
    Args:
        dag_run: DAG run containing calculated timeoff types
        current_date_json: Current date already parsed in JSON format for Replicon API
        is_rehire: Boolean indicating if this is a rehire scenario
        
    Returns:
        Dict with keys 'new_timeoff_types' and 'disable_timeoff_types' containing URIs
    """
    # Check if timeoff types should be updated
    # Per tech spec: time off types are based on org structure, update when location changes
    if not get_updated_location(dag_run):
        return {"new_timeoff_types": [], "all_timeoff_types": [], "disable_timeoff_types": []}
    
    # Get calculated timeoff types from master DAG
    calculated_timeoff_types = dag_run.conf.get("calculated_time_off_types", {})
    if not calculated_timeoff_types:
        return {"new_timeoff_types": [], "all_timeoff_types": [], "disable_timeoff_types": []}
    
    # Get current user timeoff types
    user_details_result = rail.result("get_user_details", {})
    if not user_details_result:
        return {"new_timeoff_types": [], "all_timeoff_types": [], "disable_timeoff_types": []}

    timeoff_policy_summary = user_details_result.get("timeOffTypePolicySummary", {})
    current_timeoff_assignments = timeoff_policy_summary.get("policiesByTimeOffType", [])
    
    # Extract current timeoff type names and URIs
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
    
    # Only consider currently active timeoff types (where isTimeOffAllowedAgainstThisTimeOffType is true)
    current_active_types = set()
    
    for type_name, type_info in current_timeoff_dict.items():
        if type_info["is_allowed"]:
            current_active_types.add(type_name)
    
    new_types = set(calculated_timeoff_types.keys())
    
    # Calculate only removed types (types to disable)
    removed_types = current_active_types - new_types
    # Calculate only added types (types not currently active)
    added_types = new_types - current_active_types
    
    # Build URIs for disabled types
    disable_uris = []
    for type_name in removed_types:
        if type_name in current_timeoff_dict:
            disable_uris.append(current_timeoff_dict[type_name]["uri"])
    
    # Build URIs for new types (only the ones that need to be added)
    new_uris = []
    for type_name in added_types:
        if type_name in calculated_timeoff_types:
            new_uris.append(calculated_timeoff_types[type_name])
    
    # Build URIs for all calculated timeoff types (complete new list)
    all_new_uris = []
    for type_name in calculated_timeoff_types:
        all_new_uris.append(calculated_timeoff_types[type_name])
    
    return {
        "new_timeoff_types": new_uris,          # Only timeoff types that need to be added
        "all_timeoff_types": all_new_uris,      # All calculated timeoff types
        "disable_timeoff_types": disable_uris
    }

def get_user_oefs_for_add_update(dag_run, oef_field_mapper, is_update=false):
    """
    Build OEF (Other Extension Fields) payload for add or update operations
    Args:
        dag_run: The DAG run object
        is_update: Boolean flag to indicate if this is an update operation
    Returns: dict with keys 'payload', 'missing_oefs', 'missing_oef_tags', 'updated_oefs'
    """
    
    oefs = []
    missing_oefs = []
    missing_oef_tags = []
    updated_oefs = []

    user_data = dag_run.conf
    processed_oef_data = dag_run.conf.get('oef_data', {})
    
    # Get current OEF values if this is an update
    current_oef_values = {}
    if is_update:
        user_details_result = rail.result("get_user_details", {})
        if user_details_result and user_details_result.get("userDetails"):
            current_extension_fields = user_details_result.get("userDetails", {}).get("extensionFieldValues", [])
            for field in current_extension_fields:
                oef_uri = field.get("definition", {}).get("uri")
                if oef_uri:
                    current_oef_values[oef_uri] = field
    
    # Helper function to check if OEF needs update
    def needs_update(oef_uri, new_value, value_type):
        if not is_update or not oef_uri:
            return true  # Always include in add operation
        
        current_field = current_oef_values.get(oef_uri, {})
        
        if value_type == "text":
            return current_field.get("textValue") != new_value
        elif value_type == "number":
            return str(current_field.get("numericValue")) != new_value
        elif value_type == "dropdown":
            return current_field.get("tag", {}).get("uri") != new_value
        
        return false
    
    # Process each OEF using the mapper
    for oef_config in oef_field_mapper:
        field_name = oef_config['field_name']
        oef_name = oef_config['oef_name']
        oef_type = oef_config['type']
        
        # Check if field name exists in user data (allows null, empty string, 0, false)
        if field_name not in user_data:
            continue
            
        new_value = user_data.get(field_name)
            
        # Check if OEF exists in processed data
        if not processed_oef_data.get(oef_name) or not processed_oef_data[oef_name].get('oef_uri'):
            if not is_update or needs_update(null, null, oef_type):
                missing_oefs.append(f"{oef_name} OEF is not present in Replicon")
            continue
        
        oef_uri = processed_oef_data[oef_name]['oef_uri']
        
        # Build the OEF payload based on type
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
                oef_payload["value"]["textValue"] = new_value
                if is_update:
                    updated_oefs.append(f"{oef_name} updated")
                oefs.append(oef_payload)
                
        elif oef_type == 'number':
            if needs_update(oef_uri, new_value, oef_type):
                oef_payload["value"]["numericValue"] = float(new_value)
                if is_update:
                    updated_oefs.append(f"{oef_name} updated")
                oefs.append(oef_payload)
                
        elif oef_type == 'dropdown':
            # Handle dropdown - check if new_value is null/empty for clearing
            if new_value is null or new_value == "":
                # Clear dropdown value
                if needs_update(oef_uri, null, oef_type):
                    oef_payload["value"]["tag"] = null
                    oefs.append(oef_payload)
            else:
                oef_tag_uri = processed_oef_data[oef_name].get('oef_tag_uri')
                if oef_tag_uri:
                    if needs_update(oef_uri, oef_tag_uri, oef_type):
                        oef_payload["value"]["tag"] = {"uri": oef_tag_uri}
                        if is_update:
                            updated_oefs.append(f"{oef_name} updated")
                        oefs.append(oef_payload)
                else:
                    if not is_update or needs_update(oef_uri, null, oef_type):
                        missing_oef_tags.append(f"{oef_name} OEF - Dropdown value {new_value} not present in Replicon")
    
    # Only return logs if there are actual updates
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

def get_user_custom_fields_for_add_update(dag_run, custom_field_mapper, REP_DATE_FORMAT, is_update=false):
    """
    Build Custom Fields payload for add or update operations
    Args:
        dag_run: The DAG run object
        custom_field_mapper: Custom field mapping configuration
        is_update: Boolean flag to indicate if this is an update operation
    Returns: dict with keys 'payload', 'missing_custom_fields', 'updated_custom_fields'
    """
    
    custom_fields = []
    missing_custom_fields = []
    updated_custom_fields = []
    
    user_data = dag_run.conf
    
    # Get current custom field values if this is an update
    current_date_of_employment = None
    if is_update:
        user_details_result = rail.result("get_user_details", {})
        if user_details_result and user_details_result.get("userDetails"):
            current_custom_fields = user_details_result.get("userDetails", {}).get("customFieldValues", [])
            for field in current_custom_fields:
                if field.get("customField", {}).get("name") == "Date of Employment":
                    current_date = field.get("date")
                    if current_date:
                        current_date_of_employment = f'{current_date["year"]}/{current_date["month"]:02d}/{current_date["day"]:02d}'
                    break
    
    # Date of Employment - Date Custom Field
    if 'date_of_employment' in user_data:
        new_date_value = user_data.get('date_of_employment')
        custom_field_name = custom_field_mapper.get('date_of_employment')
        
        if new_date_value is null or new_date_value == "":
            # Clear date field - only if current value is not already null
            needs_update = not is_update or current_date_of_employment is not null
            if needs_update:
                custom_fields.append({
                    "value": {
                        "customField": {
                            "uri": null,
                            "name": custom_field_name
                        },
                        "text": null,
                        "date": null,
                        "dropDownOption": null,
                        "number": null
                    }
                })
                
                if is_update:
                    updated_custom_fields.append(f"{custom_field_name} cleared")
        else:
            # Set date field
            new_date = rail.parse_date(new_date_value, REP_DATE_FORMAT)
            new_date_str = f'{new_date["year"]}/{new_date["month"]:02d}/{new_date["day"]:02d}'
            
            # Check if update is needed
            needs_update = not is_update or current_date_of_employment != new_date_str
            
            if needs_update:
                custom_fields.append({
                    "value": {
                        "customField": {
                            "uri": null,
                            "name": custom_field_name
                        },
                        "text": null,
                        "date": new_date,
                        "dropDownOption": null,
                        "number": null
                    }
                })
                
                if is_update:
                    updated_custom_fields.append(f"{custom_field_name} updated")
    
    return {
        'payload': custom_fields,
        'missing_custom_fields': missing_custom_fields,
        'updated_custom_fields': updated_custom_fields
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
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_updated_logs(dag_run, config):
    """
    Get comprehensive update logs from all relevant functions
    Returns list of successful updates made to the user
    """
    all_updates = []
    
    # Get logs from basic user details update
    basic_update_result = get_basic_user_details_update(dag_run, config.YMD_DATE_FORMAT, config.REP_DATE_FORMAT)
    basic_details_logs = basic_update_result.get('basic_details_logs', [])
    all_updates.extend(basic_details_logs)
    
    # Get logs from OEF updates (only successful updates, not exceptions)
    oef_field_mapper = config.oef_field_mapper_data
    oef_result = get_user_oefs_for_add_update(dag_run, oef_field_mapper, is_update=true)
    updated_oefs = oef_result.get('updated_oefs', [])
    
    if updated_oefs:
        all_updates.extend(updated_oefs)
    
    # Get logs from custom field updates
    custom_field_mapper = config.custom_field_mapper_data
    custom_field_result = get_user_custom_fields_for_add_update(dag_run, custom_field_mapper, config.REP_DATE_FORMAT, is_update=true)
    updated_custom_fields = custom_field_result.get('updated_custom_fields', [])
    
    if updated_custom_fields:
        all_updates.extend(updated_custom_fields)
    
    # Get logs from group updates
    if get_updated_location(dag_run):
        all_updates.append("Location updated")
    
    if get_updated_department(dag_run):
        all_updates.append("Cost center updated")
    
    if get_updated_servicecenter(dag_run):
        all_updates.append("Department updated")
    
    if get_updated_supervisor(dag_run):
        all_updates.append("Supervisor updated")
    
    if get_updated_holiday_calendar(dag_run):
        all_updates.append("Holiday calendar updated")
    
    # Get logs from employee type updates
    employee_type_mapper = config.employee_type_mapper_data
    updated_employee_type = get_updated_employeetype(dag_run, employee_type_mapper)
    if updated_employee_type:
        all_updates.append("Employee type updated")
    
    # Get logs from timesheet template updates
    timesheet_template_mapper = config.timesheet_template_mapper_data
    updated_template = get_updated_timesheet_template(dag_run, timesheet_template_mapper)
    if updated_template:
        all_updates.append("Timesheet template updated")
    
    # Get logs from permission updates
    employee_type_mapper = config.employee_type_mapper_data
    permission_updates = get_updated_permissions(dag_run, config.permissions_mapper_data, employee_type_mapper)
    if permission_updates:
        all_updates.append("Permissions updated")
    
    # Get logs from timeoff type updates
    timeoff_changes = get_updated_timeoff_types(dag_run)
    if timeoff_changes["new_timeoff_types"]:
        all_updates.append("Time off types updated")
    
    return all_updates

def get_action_type(dag_run, config):
    """
    Determine if this is a Rehire, Disable, or Update action
    Returns 'Rehire' if rehire conditions are met, 'Disable' if user is being disabled, otherwise 'Update'
    """
    basic_update_result = get_basic_user_details_update(dag_run, config.YMD_DATE_FORMAT, config.REP_DATE_FORMAT)
    is_rehire = basic_update_result.get('is_rehire', false)
    is_disabled = basic_update_result.get('is_disabled', false)
    
    if is_rehire:
        return "Rehire"
    elif is_disabled:
        return "Disable"
    else:
        return "Update"

def check_exception_rules(exception, current_target_value, target_field, user_attrs):
    """
    Check if an exception rule matches current user attributes
    Returns True if exception applies (should preserve current value)
    """
    org_structure = user_attrs['org_structure']
    work_relationship = user_attrs['work_relationship']
    employment_type = user_attrs['employment_type']
    employment_subtype = user_attrs['employment_subtype']
    manager_flag = user_attrs['manager_flag']
    current_employee_type = user_attrs['current_employee_type']
    
    # Check if this exception matches the current target value
    if exception.get(target_field) != current_target_value:
        return False
        
    # Check org structure
    if exception.get('org_structure_code') != org_structure:
        return False
        
    # Check work relationship - can be string or list (handle both cases for consistency)
    work_relationship_constraint = exception.get('work_relationship')
    if work_relationship_constraint:
        # If it's a list, check if work_relationship is in the list
        if isinstance(work_relationship_constraint, list):
            if work_relationship_constraint and work_relationship not in work_relationship_constraint:
                return False
        # If it's a string, do direct comparison
        elif isinstance(work_relationship_constraint, str):
            if work_relationship_constraint != work_relationship:
                return False
    
    # Check employment type
    employment_types_include = exception.get('employment_type_include', [])
    employment_types_exclude = exception.get('employment_type_exclude', [])
    
    if employment_types_include:
        if employment_type not in employment_types_include:
            return False
    elif employment_types_exclude:
        if employment_type in employment_types_exclude:
            return False
    
    # Check employment subtype
    subtype_include = exception.get('employment_subtype_include', [])
    subtype_exclude = exception.get('employment_subtype_exclude', [])
    
    if subtype_include:
        if employment_subtype not in subtype_include:
            return False
    elif subtype_exclude:
        if employment_subtype in subtype_exclude:
            return False
    
    # Check manager flag - empty list means allow all values
    manager_flags = exception.get('manager_flag', [])
    if manager_flags and manager_flag not in manager_flags:
        return False
    
    # Check employee type (for timesheet template exceptions only)
    # Employee type exceptions should not check this field since it's the target field being preserved
    if target_field != 'employee_type':
        employee_type_constraint = exception.get('employee_type')
        if employee_type_constraint:
            # Handle both string and list formats
            if isinstance(employee_type_constraint, list):
                if employee_type_constraint and current_employee_type not in employee_type_constraint:
                    return False
            elif isinstance(employee_type_constraint, str):
                if employee_type_constraint != current_employee_type:
                    return False
    
    # All conditions matched
    return True

def get_user_attributes_from_replicon(dag_run):
    """
    Extract user attributes from current Replicon data (user_details and group_membership)
    This ensures consistent data source across all functions that check exception rules
    """
    current_group_membership = rail.result("get_current_group_membership", {})
    user_details_result = rail.result("get_user_details", {})
    user_details = user_details_result.get("userDetails", {}) if user_details_result else {}
    
    # Get org structure from current location
    org_structure = rail.find_first_by_attr_and_get_attr(rail.load_all_records(
        dag_run.conf.get("replicon_org_structures")), "uri", current_group_membership.get("existinglocationuri"), "code")
    
    # Get employment details from extension fields (use correct field type)
    work_relationship = rail.find_first_by_attr_and_get_attr(
        user_details.get("extensionFieldValues", []), 
        "definition.displayText", "Type of work relationship", "textValue")
    employment_type = rail.find_first_by_attr_and_get_attr(
        user_details.get("extensionFieldValues", []), 
        "definition.displayText", "Type of employment", "textValue")
    employment_subtype = rail.find_first_by_attr_and_get_attr(
        user_details.get("extensionFieldValues", []), 
        "definition.displayText", "Sub type of employment", "textValue")
    manager_flag = rail.find_first_by_attr_and_get_attr(
        user_details.get("extensionFieldValues", []), 
        "definition.displayText", "Manager flag", "tag.displayText")
    
    # Get current employee type
    current_employee_type = current_group_membership.get("existingemployeetypename", "") if current_group_membership else ""
    
    return {
        'org_structure': org_structure,
        'work_relationship': work_relationship,
        'employment_type': employment_type,
        'employment_subtype': employment_subtype,
        'manager_flag': manager_flag,
        'current_employee_type': current_employee_type,
        'current_group_membership': current_group_membership,
        'user_details_result': user_details_result
    }

def get_exception_logs(dag_run, is_update_scenario, config):
    """
    Get comprehensive exception logs from all relevant functions
    Returns list of exception messages including missing OEFs, preserved mappings, etc.
    Works for both ADD and UPDATE scenarios - is_update_scenario parameter indicates context
    """
    all_exceptions = []
    
    # Get exceptions from OEF processing (missing OEFs and tags)
    # Only check for add scenario OEF issues or update scenario issues based on context
    oef_field_mapper = config.oef_field_mapper_data
    oef_result = get_user_oefs_for_add_update(dag_run, oef_field_mapper, is_update=is_update_scenario)
    missing_oefs = oef_result.get('missing_oefs', [])
    missing_oef_tags = oef_result.get('missing_oef_tags', [])
    
    all_exceptions.extend(missing_oefs)
    all_exceptions.extend(missing_oef_tags)

    if dag_run.conf.get("orgstructure") and not dag_run.conf.get("calculated_orgstructure_uri"):
        all_exceptions.append(f"Org Structure \'{dag_run.conf.get('orgstructure')}\' is not present in Replicon")

    if dag_run.conf.get("costcenter") and not dag_run.conf.get("calculated_costcenter_uri"):
        all_exceptions.append(f"Cost center \'{dag_run.conf.get('costcenter')}\' is not present in Replicon")

    if dag_run.conf.get("calculated_department") and not dag_run.conf.get("calculated_department_uri"):
        all_exceptions.append(f"Department \'{dag_run.conf.get('calculated_department')}\' is not present in Replicon")

    if dag_run.conf.get("calculated_employee_type") and not dag_run.conf.get("calculated_employee_type_uri"):
        all_exceptions.append(f"Employee type \'{dag_run.conf.get('calculated_employee_type')}\' is not present in Replicon")

    if dag_run.conf.get("calculated_timesheet_template") and not dag_run.conf.get("calculated_timesheet_template_uri"):
        all_exceptions.append(f"Timesheet template \'{dag_run.conf.get('calculated_timesheet_template')}\' is not present in Replicon")
    
    # Only check preservation exceptions for UPDATE scenarios
    if is_update_scenario:
        # Get common user details and attributes for both employee type and timesheet template checks
        employee_type_mapper = config.employee_type_mapper_data
        timesheet_template_mapper = config.timesheet_template_mapper_data
        
        # Get user attributes from Replicon using shared function for consistency
        user_attrs = get_user_attributes_from_replicon(dag_run)
        current_employee_type = user_attrs['current_employee_type']
        user_details_result = user_attrs['user_details_result']
        
        # Check if current employee type should be preserved based on exception rules
        if (employee_type_mapper and 
            'exceptions' in employee_type_mapper and 
            current_employee_type):
            
            # Check exception rules using shared function
            for exception in employee_type_mapper['exceptions']:
                if check_exception_rules(exception, current_employee_type, 'employee_type', user_attrs):
                    all_exceptions.append(f"Employee type '{current_employee_type}' preserved due to exception rules")
                    break
        
        # Check for timesheet template preservation exceptions using mapper
        current_timesheet_template_obj = user_details_result.get("timesheetTemplate", {}) if user_details_result else {}
        current_timesheet_template = current_timesheet_template_obj.get("name", "") if current_timesheet_template_obj else ""
        
        # Check if current timesheet template should be preserved based on exception rules
        if (timesheet_template_mapper and 
            'exceptions' in timesheet_template_mapper and 
            current_timesheet_template):
            
            # Check exception rules using shared function
            for exception in timesheet_template_mapper['exceptions']:
                if check_exception_rules(exception, current_timesheet_template, 'timesheet_template', user_attrs):
                    all_exceptions.append(f"Timesheet template '{current_timesheet_template}' preserved due to exception rules")
                    break

    # Common validations for both ADD and UPDATE scenarios
    if dag_run.conf.get("supervisorempid") and not rail.result("get_supervisor_details") and dag_run.conf.get("supervisorempid") not in custom_methods.get_all_user_employee_ids_from_feed(dag_run):
        all_exceptions.append(f'Supervisor {dag_run.conf.get("supervisorempid")} is not available in Replicon')
    if dag_run.conf.get("employeeid") == dag_run.conf.get("supervisorempid"):
        all_exceptions.append("Supervisor ID cannot be the same as employee ID")
    
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

def get_updated_permissions(dag_run, permissions_mapper, employee_type_mapper):
    """
    Check for permission updates based on org structure/employee type changes
    Returns permission schedule payload for add/remove operations
    Per tech spec: permissions only update when org structure or employee type changes
    """
    # Check if org structure or employee type changed first (per tech spec)
    if not (get_updated_location(dag_run) or get_updated_employeetype(dag_run, employee_type_mapper)):
        return []
    
    # Get calculated permissions from master DAG
    calculated_permissions = dag_run.conf.get("calculated_permissions", {})
    if not calculated_permissions:
        return []
    
    # Get current user permissions
    user_details_result = rail.result("get_user_details", {})
    current_user = user_details_result.get("userDetails", {}) if user_details_result else {}
    current_permissions = current_user.get("permissionSets", [])
    
    # Convert current permissions to dict for easy comparison
    current_permission_dict = {}
    for perm_set in current_permissions:
        policy = perm_set.get("permissionSetPolicy", {})
        name = policy.get("name")
        uri = policy.get("uri")
        if name and uri:
            current_permission_dict[name] = uri
    
    # Get all permissions defined in the permissions mapper for filtering
    mapper_permissions = set()
    for mapping in permissions_mapper:
        for permission in mapping.get("permission", []):
            mapper_permissions.add(permission)
    
    # If user has no permissions currently, add all calculated permissions
    if not current_permission_dict:
        if calculated_permissions:
            return [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "uri": uri,
                                "name": name
                            },
                            "groupAccessFilter": null
                        }
                        for name, uri in calculated_permissions.items()
                    ]
                }
            ]
        return []
    
    # Compare current and calculated permissions
    permissions_to_add = []
    permissions_to_remove = []
    
    # Find permissions to add (in calculated but not in current)
    for perm_name, perm_uri in calculated_permissions.items():
        if perm_name not in current_permission_dict:
            permissions_to_add.append({
                "permissionSetPolicy": {
                    "uri": perm_uri,
                    "name": perm_name
                },
                "groupAccessFilter": null
            })
    
    # Find permissions to remove (in current but not in calculated)
    # Only consider permissions that are defined in the permissions mapper
    for perm_name, perm_uri in current_permission_dict.items():
        if (perm_name not in calculated_permissions and 
            perm_name in mapper_permissions):
            permissions_to_remove.append({
                "permissionSetPolicy": {
                    "uri": perm_uri,
                    "name": perm_name
                },
                "groupAccessFilter": null
            })
    
    # Build the schedule payload
    schedule = []
    
    if permissions_to_add:
        schedule.append({
            "modificationOptionUri": "urn:replicon:collection-modification-option:add",
            "items": permissions_to_add
        })
    
    if permissions_to_remove:
        schedule.append({
            "modificationOptionUri": "urn:replicon:collection-modification-option:remove",
            "items": permissions_to_remove
        })
    
    return schedule

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


def get_supervisor_assignment_payload(dag_run, YMD_DATE_FORMAT):
    """Get payload for supervisor assignment in supervisor assignment child DAG"""
    
    supervisor_uri = rail.result("get_supervisor_details")["userDetails"]["uri"]
    current_date = rail.parse_date(dag_run.conf["current_date"], YMD_DATE_FORMAT)
    
    # Check if user has existing supervisor
    supervisor_assignment = rail.result("get_supervisor_assignment_details")
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
        "unitOfWorkId": str(uuid.uuid4())
    }
