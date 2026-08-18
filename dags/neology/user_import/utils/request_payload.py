from datetime import datetime, date
import uuid
import pendulum
import rail
from airflow.models import Variable
from neology.user_import.utils import custom_methods
null = None
true = True
false = False
EFFECTIVE_DATE_FORMAT_BAMBOOHR = '%Y-%m-%d'

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

def get_prior_saturday(date_json):
    target_date = date(date_json["year"], date_json["month"], date_json["day"])
    weekday = target_date.weekday()

    if weekday == 5:
        return {
            "year": target_date.year,
            "month": target_date.month,
            "day": target_date.day
        }

    if weekday == 6:
        days_to_subtract = 1
    else:
        days_to_subtract = weekday + 2

    prior_saturday = target_date - pendulum.duration(days=days_to_subtract)
    return {
        "year": prior_saturday.year,
        "month": prior_saturday.month,
        "day": prior_saturday.day
    }

def get_bamboohr_employees_request(required_employee_fields):
    """
    Build BambooHR employee data request payload.
    Uses bamboohr_field from the matched fields (from fields API).
    """
    last_modified_time = rail.result('get_lastsync_time_and_current_time')['last_synctime']
    return {
        "filters": {
            "filters": [
                {
                    "field": "lastChanged",
                    "operator": "gte",
                    "value": last_modified_time
                }
            ],
            "match": "all"
        },
        "fields": [field["bamboohr_field"] for field in required_employee_fields if field.get("bamboohr_field")]
    }

def get_login_enabled_status(dag_run):
    """Determine if user login should be enabled based on hire date"""
    # If hire date is blank, login should be disabled
    hire_date = dag_run.conf["user_details"].get("hiredate")
    if not hire_date:
        return False
    
    # Parse hire date and current date
    hire_date_obj = datetime.strptime(hire_date, EFFECTIVE_DATE_FORMAT_BAMBOOHR).date()
    current_date_json = dag_run.conf["process_start_time"]
    current_date = date(current_date_json["year"], current_date_json["month"], current_date_json["day"])
    
    # If hire date is in the future, login should be disabled
    if hire_date_obj > current_date:
        return False
    
    return True

def get_process_user_conf(item, required_employee_fields):
    timeoff_lookup = {timeoff["displayText"]: timeoff["uri"] for timeoff in rail.result("get_all_time_off_types")}
    timesheet_type = item.get("timesheettype")
    punch_entry_policy = "TOMS Punch Entry policy" if (timesheet_type and "punch" in timesheet_type.lower() and item.get("subsidiary") == "TOMS") else "Default Punch Entry policy"
    return {
        "user_details": item,
        "process_start_time": rail.result("logging_details")["current_time_json"],
        "oef_uris": rail.result("get_all_user_oefs"),
        "oef_fields": [field for field in required_employee_fields if field["type"] == "oef"],
        "last_synctime": rail.result('get_lastsync_time_and_current_time')['last_synctime'],
        "holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_holiday_calendars"), "displayText", item["holidaycalendar"], "uri"),
        "timesheet_template_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policysets"), "displayText", item["timesheettype"], "uri"),
        "punch_entry_policy_name": punch_entry_policy,
        "punch_entry_policy_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policysets"), "displayText", punch_entry_policy, "uri"),
        "payrule_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_payrule_scripts"), "displayText", item["payrule"], "uri"),
        "timezone_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_time_zones"), "displayText", item["timezone"], "uri"),
        "timeoff_types": [
            {
                "name": timeoff_type_name.strip(),
                "uri": timeoff_lookup.get(timeoff_type_name.strip())
            } for timeoff_type_name in item["joblevel_oef"].split(";")
        ] if item.get("joblevel_oef") else [],
        "project_role_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_project_roles"), "displayText", item["ratecode_oef"], "uri"),
        "office_schedule_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_office_schedules"), "displayText", item["subsidiary"], "uri"),
        "location_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_locations"), "displayText", item["location"], "uri"),
        "department_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_departments"), "displayText", item["department"], "uri"),
        "division_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_divisions"), "displayText", item["subsidiary"], "uri"),
        "employeetype_oef_tag_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_employee_type_oef_tags"), "oef_tag", item.get("employeetype_oef", ""), "uri") if item.get("employeetype_oef") else None,
        "agency_oef_tag_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_agency_oef_tags"), "oef_tag", item.get("agency_oef", ""), "uri") if item.get("agency_oef") else None,
        "adpcompanycode_oef_tag_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_adp_company_code_oef_tags"), "oef_tag", item.get("adpcompanycode_oef", ""), "uri") if item.get("adpcompanycode_oef") else None
    }

def get_assign_supervisor_permission_payload(supervisor_permissions):
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
                                "name": supervisor_permissions
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

def get_oef_details_to_add(dag_run):
    oef_data_to_add = []
    oef_uris = dag_run.conf["oef_uris"]
    for oef_data in dag_run.conf["oef_fields"]:
        # Filter by action: only "add" or "add_update"
        if oef_data.get("action") not in ["add", "add_update"]:
            continue

        oef_key = oef_data["field_attr"]

        # Special handling for BambooHR Integration - hardcode to "True"
        if oef_key == "bamboohr_integration_oef":
            if oef_uris.get(oef_key):
                oef_data_to_add.append({
                    "definition": {
                        "uri": oef_uris[oef_key],
                        "name": null
                    },
                    "tag": null,
                    "numericValue": null,
                    "textValue": "True",
                    "fileValue": null,
                    "jsonValue": null
                })
            continue

        if oef_uris.get(oef_key):
            user_value = dag_run.conf["user_details"].get(oef_data["field_attr"], "")

            # Check if this is a list OEF and has a tag URI
            if oef_data.get("oef_type") == "list" and user_value:
                # Map field_attr to tag URI key
                tag_uri_key = f"{oef_key}_tag_uri"
                tag_uri = dag_run.conf.get(tag_uri_key)

                if tag_uri:
                    # For list OEFs with tag URI, use tag
                    oef_data_to_add.append({
                        "definition": {
                            "uri": oef_uris[oef_key],
                            "name": null
                        },
                        "tag": {
                            "uri": tag_uri,
                            "slug": null,
                            "tagName": null
                        },
                        "numericValue": null,
                        "textValue": null,
                        "fileValue": null,
                        "jsonValue": null
                    })
                # If no tag URI found, skip this OEF (will be logged as exception)
            else:
                # For text OEFs, use textValue
                oef_data_to_add.append({
                    "definition": {
                        "uri": oef_uris[oef_key],
                        "name": null
                    },
                    "tag": null,
                    "numericValue": null,
                    "textValue": user_value,
                    "fileValue": null,
                    "jsonValue": null
                })

    return oef_data_to_add

def get_oef_details_to_update(dag_run):
    oef_data_to_update = []
    for oef_data in dag_run.conf["oef_fields"]:
        # Filter by action: only "add_update"
        if oef_data.get("action") != "add_update":
            continue

        oef_key = oef_data["field_attr"]
        bamboohr_field = oef_data["bamboohr_name"]
        if not bamboohr_field or not dag_run.conf["user_details"].get(oef_key):
            continue
        user_value = dag_run.conf["user_details"][oef_key]
        oef_uri = dag_run.conf["oef_uris"].get(oef_key)
        if not oef_uri:
            continue
        current_replicon_value = rail.find_first_by_attr_and_get_attr(
            rail.result("get_user_details_from_replicon")["userDetails"]["extensionFieldValues"],
            "definition.displayText", oef_data["replicon_name"], "textValue"
        )
        if user_value != current_replicon_value:
            if oef_data.get("oef_type") == "list" and user_value:
                # Map field_attr to tag URI key
                tag_uri_key = f"{oef_key}_tag_uri"
                tag_uri = dag_run.conf.get(tag_uri_key)
                oef_data_to_update.append({
                    "definition": {
                        "uri": oef_uri,
                        "name": null
                    },
                    "tag": {
                        "uri": tag_uri,
                        "slug": null,
                        "tagName": null
                    },
                    "numericValue": null,
                    "textValue": null,
                    "fileValue": null,
                    "jsonValue": null
                })

            else:
                oef_data_to_update.append({
                    "definition": {
                        "uri": oef_uri,
                        "name": null
                    },
                    "tag": null,
                    "numericValue": null,
                    "textValue": user_value,
                    "fileValue": null,
                    "jsonValue": null
                })
    return oef_data_to_update

def get_create_user_payload(dag_run, user_permission_set, supervisor_permission_set, replicon_default_password, all_license_types, licenses, all_notifications):
    auth_type = "sso" if any(domain in dag_run.conf['user_details']['workemail'].lower()
        for domain in ['neology', '@neopartners.mx', '@controelec.com']) else "replicon"
    valid_timeoff_types = [timeoff_type for timeoff_type in dag_run.conf["timeoff_types"] if timeoff_type["uri"]]
    current_date = dag_run.conf["process_start_time"]
    return {
        "target": null,
        "template": {
            "templateTarget": null
        },
        "modifications": {
            "firstName": {
                "value": dag_run.conf["user_details"]["firstname"]
            },
            "lastName": {
                "value": dag_run.conf["user_details"]["lastname"]
            },
            "loginName": {
                "value": dag_run.conf["user_details"]["workemail"] if auth_type == "sso"
                    else dag_run.conf["user_details"]["employeenumber"]
            },
            "displayName": {
                "value": dag_run.conf["user_details"].get("preferredname") if dag_run.conf["user_details"].get("preferredname") 
                    else f'{dag_run.conf["user_details"]["lastname"]}, {dag_run.conf["user_details"]["firstname"]}'
            },
            "emailAddress": {
                "value": dag_run.conf["user_details"]["workemail"]
            },
            "employeeId": {
                "value": dag_run.conf["user_details"]["employeenumber"]
            },
            "employmentDateRange": {
                "value": {
                    "startDate": rail.parse_date(dag_run.conf["user_details"]["hiredate"], EFFECTIVE_DATE_FORMAT_BAMBOOHR)
                        if dag_run.conf["user_details"]["hiredate"] else null,
                    "endDate": rail.parse_date(dag_run.conf["user_details"]["terminationdate"], EFFECTIVE_DATE_FORMAT_BAMBOOHR)
                        if dag_run.conf["user_details"]["terminationdate"] else null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            } if dag_run.conf["user_details"]["hiredate"] or dag_run.conf["user_details"]["terminationdate"] else null,
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": get_login_enabled_status(dag_run)
                    },
                    "forcePasswordChange": {
                        "value": true
                    } if auth_type == "replicon" else null,
                    "ssoName": {
                        "value": dag_run.conf["user_details"]["workemail"]
                    } if auth_type == "sso" else null,
                    "ssoNameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name"
                        if auth_type == "sso" else null,
                    "password": {
                        "value": Variable.get(replicon_default_password, default_var=null) 
                    } if auth_type == "replicon" else null,
                    "authenticationProviders": [
                        {
                            "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                            "items": [
                                {
                                    "name": "Replicon Internal"
                                }
                            ]
                        }
                    ] if auth_type == "replicon" else [],
                    "emailMFAResendVerificationEmail": null,
                    "emailMFATryAddMethodFromUsersEmail": null,
                    "isMFAMethodRequired": null,
                    "clearIsLockedOut": null
                }
            },
            "timesheetApprovalPath": {
                "value": {
                    "uri": null,
                    "name": "Escalated to next supervisor after elapsed time"
                }
            },
            "workAuthorizationApprovalPath": null,
            "timeoffApprovalPath": {
                "value": {
                    "uri": null,
                    "name": "Supervisor"
                }
            },
            "timeOffBalancePayoutApprovalPath": null,
            "defaultActivity": null,
            "expenseApprovalPath": null,
            "timeZone": {
                "value": {
                    "uri": dag_run.conf["timezone_uri"],
                    "IANAName": null
                }
            } if dag_run.conf["timezone_uri"] else null,
            "workWeekStartDay": {
                "value": {
                    "uri": "urn:replicon:day-of-week:saturday"
                }
            },
            "defaultBillingRate": null,
            "userPreferences": null,
            "formattings": null,
            "notificationPreferences": {
                "value": {
                    "notificationDeliveryPreferences": [
                        {
                          "objectTypeUri": f"urn:replicon:object-type:{category}",
                          "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        } for category in all_notifications
                    ],
                    "sharedDeliveryPreferenceOptionUris": [
                        "urn:replicon:user-shared-delivery-preference-option:always-deliver"
                    ]
                }
            },
            "timesheetTemplate": null,
            "policySetsScheduleToApply": [
                {
                    "policyUri": "urn:replicon:policy:timesheet",
                    "schedule": [
                        {
                            "policySetUri": dag_run.conf["timesheet_template_uri"],
                            "effectiveDate": get_prior_saturday(rail.parse_date(dag_run.conf["user_details"]["hiredate"], EFFECTIVE_DATE_FORMAT_BAMBOOHR)),
                        }
                    ]
                }
            ] if dag_run.conf["timesheet_template_uri"] else [],
            "timeoffTemplate": {
                "value": {
                    "uri": null,
                    "name": "Time Off"
                }
            },
            "timeOffCalendarVisibility": null,
            "expenseTemplate": null,
            "workAuthorizationTemplate": null,
            "punchEntryPolicy": {
                "value": {
                    "uri": dag_run.conf["punch_entry_policy_uri"],
                    "name": null
                }
            } if dag_run.conf["punch_entry_policy_uri"] else null,
            "holidayCalendar": {
                "value": {
                    "uri": dag_run.conf["holiday_calendar_uri"],
                    "name": null
                }
            } if dag_run.conf["holiday_calendar_uri"] else null,
            "extensionFields": [
                {
                    "value": oef_item
                } for oef_item in get_oef_details_to_add(dag_run)
            ],
            "customFields": [],
            "products": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:remove",
                    "items": [
                        {
                            "uri": null,
                            "name": license_name
                        }
                    ]
                } for license_name in all_license_types if license_name not in licenses
            ],
            "skills": [],
            "activities": [],
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "uri": null,
                                "name": permission_role
                            },
                            "groupAccessFilter": null
                        } for permission_role in (supervisor_permission_set if dag_run.conf["user_details"]["issupervisor"] == "true"
                            else user_permission_set)
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
                                "uri": timeoff_type["uri"],
                                "name": null
                            },
                            "isTimeOffAllowedAgainstThisTimeOffType": true,
                            "applyDefaultTimeOffTypePolicy": true,
                            "defaultTimeOffTypePolicyEffectiveDate": rail.parse_date(dag_run.conf["user_details"]["hiredate"], EFFECTIVE_DATE_FORMAT_BAMBOOHR)
                                if dag_run.conf["user_details"]["hiredate"] else dag_run.conf["process_start_time"],
                            "policySchedule": []
                        } for timeoff_type in valid_timeoff_types
                    ]
                }
            ] if valid_timeoff_types else [],
            "locationSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": dag_run.conf["location_uri"],
                        "parentUri": null,
                        "name": null
                    }
                }
            ] if dag_run.conf["location_uri"] else [],
            "divisionSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": dag_run.conf["division_uri"],
                        "parentUri": null,
                        "name": null
                    }
                }
            ] if dag_run.conf["division_uri"] else [],
            "departmentGroupSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": dag_run.conf["department_uri"],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    }
                }
            ] if dag_run.conf["department_uri"] else [],
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
                        "employeeId": dag_run.conf["user_details"]["supervisorid"],
                        "parameterCorrelationId": null
                    }
                }
            ] if (dag_run.conf["user_details"]["supervisorid"] and
                dag_run.conf["user_details"]["employeenumber"] != dag_run.conf["user_details"]["supervisorid"])
                    and rail.result("get_supervisor_details") else [],
            "timesheetPeriodSchedule": [
                {
                    "dateRange": {
                        "startDate": get_prior_saturday(rail.parse_date(dag_run.conf["user_details"]["hiredate"], EFFECTIVE_DATE_FORMAT_BAMBOOHR)),
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "name": "Weekly starting on Saturday"
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
                        "uri": dag_run.conf["holiday_calendar_uri"],
                        "name": null
                    }
                }
            ] if dag_run.conf["holiday_calendar_uri"] else [],
            "scheduleTypeSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                        "officeSchedule": {
                            "officeScheduleUri": dag_run.conf["office_schedule_uri"],
                            "name": null
                        }
                    }
                }
            ] if dag_run.conf["office_schedule_uri"] else [],
            "payRuleSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": dag_run.conf["payrule_uri"],
                        "name": null
                    }
                }
            ] if dag_run.conf["payrule_uri"] else [],
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
                            "uri": dag_run.conf["project_role_uri"],
                            "name": null
                        },
                        "isPrimary": true
                    }
                }
            ] if dag_run.conf["project_role_uri"] else [],
            "costNormalizationRuleSchedule": [],
            "hourlyRatesSchedule": [],
            "substituteUserSchedule": []
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_user_details_from_replicon(employeeid):
    return {
        "users": [
            {
                "employeeId": employeeid,
                "loginName": null,
                "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_updated_location(dag_run):
    return dag_run.conf["location_uri"] if (dag_run.conf.get("location_uri") and
        dag_run.conf["location_uri"] != rail.result("get_current_group_membership")["existinglocationuri"]) else null

def get_updated_department(dag_run):
    return dag_run.conf["department_uri"] if (dag_run.conf.get("department_uri") and
        dag_run.conf["department_uri"] != rail.result("get_current_group_membership")["existingdepartmenturi"]) else null

def get_updated_division(dag_run):
    return dag_run.conf["division_uri"] if (dag_run.conf.get("division_uri") and
        dag_run.conf["division_uri"] != rail.result("get_current_group_membership")["existingdivisionuri"]) else null

def get_updated_supervisor(dag_run):
    supervisor_assignment = rail.result("get_supervisor_assignment_details")
    existing_supervisor = supervisor_assignment.get("user", {}).get("uri") if supervisor_assignment else null
    return (rail.result("get_supervisor_details")["userDetails"]["uri"] if (dag_run.conf["user_details"]["supervisorid"] != dag_run.conf["user_details"]["employeenumber"]
        and rail.result("get_supervisor_details") and rail.result("get_supervisor_details")["userDetails"]["uri"] != existing_supervisor) else null)

def get_updated_holiday_calendar(dag_run):
    return (dag_run.conf["holiday_calendar_uri"] if dag_run.conf["holiday_calendar_uri"]
        and dag_run.conf["holiday_calendar_uri"] != (rail.result("get_user_holiday_calendar")["uri"]
            if rail.result("get_user_holiday_calendar") else null) else null)

def get_updated_timezone(dag_run):
    return (dag_run.conf["timezone_uri"] if dag_run.conf["timezone_uri"] and dag_run.conf["timezone_uri"] != (
        rail.result("get_user_details_from_replicon")["timeZone"]['uri']
            if rail.result("get_user_details_from_replicon")["timeZone"] else null) else null)

def get_updated_punchentry_policy(dag_run):
    return (dag_run.conf['punch_entry_policy_uri'] if dag_run.conf['punch_entry_policy_uri'] and
        dag_run.conf['punch_entry_policy_uri'] != rail.result("get_assigned_policy_sets_for_user") else null)

def get_updated_timesheet(dag_run):
    return (dag_run.conf["timesheet_template_uri"] if dag_run.conf["timesheet_template_uri"] and dag_run.conf["timesheet_template_uri"] != (
        rail.result("get_user_details_from_replicon")["timesheetTemplate"]['uri']
            if rail.result("get_user_details_from_replicon")["timesheetTemplate"] else null) else null)

def get_updated_payrule(dag_run):
    """Check if pay rule needs to be updated based on current effective pay rule"""
    user_data = rail.result("get_user_details_from_replicon")
    pay_rule_schedules = user_data.get("payRuleScriptSchedule", [])
    new_payrule_uri = dag_run.conf.get("payrule_uri")
    
    # If no pay rule URI found, no update
    if not new_payrule_uri:
        return None
    
    # If no pay rule schedules exist, need to add the new pay rule
    if not pay_rule_schedules:
        return new_payrule_uri
    
    # Get current date from process start time
    current_date_json = dag_run.conf["process_start_time"]
    current_date = date(current_date_json["year"], current_date_json["month"], current_date_json["day"])
    
    # Find the currently effective pay rule by sorting schedules by effectiveDate
    # and finding the most recent one that's not in the future
    sorted_schedules = []
    for schedule in pay_rule_schedules:
        effective_date = schedule.get("effectiveDate")
        if effective_date:
            effective_date_obj = date(effective_date["year"], effective_date["month"], effective_date["day"])
        else:
            # If no effective date, treat as beginning of time
            effective_date_obj = date.min
        
        pay_rule_script = schedule.get("payRuleScript", {})
        if pay_rule_script:
            sorted_schedules.append((effective_date_obj, pay_rule_script.get("uri")))
    
    # Sort by effective date (most recent first)
    sorted_schedules.sort(key=lambda x: x[0], reverse=True)
    
    # Find the currently effective pay rule
    current_payrule_uri = None
    for effective_date_obj, payrule_uri in sorted_schedules:
        # Use the most recent schedule that's not in the future
        if effective_date_obj <= current_date:
            current_payrule_uri = payrule_uri
            break
    
    # If no effective schedule found, use the one with no effective date or the oldest one
    if not current_payrule_uri and sorted_schedules:
        current_payrule_uri = sorted_schedules[-1][1]
    
    # Update needed if URIs are different
    return new_payrule_uri if current_payrule_uri != new_payrule_uri else None

def get_updated_schedule(dag_run):
    """Check if schedule type needs to be updated based on subsidiary/division"""
    user_data = rail.result("get_user_details_from_replicon")
    schedule_policies = user_data.get("schedulePolicies", [])
    new_office_schedule_uri = dag_run.conf.get("office_schedule_uri")
    
    # If no office schedule URI found for subsidiary, no update
    if not new_office_schedule_uri:
        return None
    
    # Get current date from process start time
    current_date_json = dag_run.conf["process_start_time"]
    current_date = date(current_date_json["year"], current_date_json["month"], current_date_json["day"])
    
    # Find the currently effective schedule
    current_schedule_uri = None
    for policy in schedule_policies:
        if policy.get("scheduleTypeUri") == "urn:replicon:schedule-type:office-schedule":
            effective_date = policy.get("effectiveDate")
            end_date = policy.get("endDate")
            
            # Parse dates if they exist
            eff_date = date(effective_date["year"], effective_date["month"], effective_date["day"]) if effective_date else date.min
            end_date_obj = date(end_date["year"], end_date["month"], end_date["day"]) if end_date else date.max
            
            # Check if this policy is currently effective
            if eff_date <= current_date <= end_date_obj:
                office_schedule = policy.get("officeSchedule")
                if office_schedule:
                    current_schedule_uri = office_schedule.get("uri")
                    break
    
    # Update needed if URIs are different
    return new_office_schedule_uri if current_schedule_uri != new_office_schedule_uri else None

def is_rehired_user(dag_run):
    return (dag_run.conf["user_details"]["status"].lower() == "active"
        and rail.result("get_user_details_from_replicon")["userDetails"]["isEnabled"] is False)

def get_updated_timeoff_types(dag_run):
   assigned_enabled_timeoffs = [policy["timeOffType"]["uri"]
      for policy in rail.result("get_user_details_from_replicon")["timeOffTypePolicySummary"]["policiesByTimeOffType"]
        if policy["isTimeOffAllowedAgainstThisTimeOffType"] is true]
   available_timeoffs = [timeoff["uri"] for timeoff in dag_run.conf["timeoff_types"] if timeoff.get("uri")]
   to_add = [timeoffuri for timeoffuri in available_timeoffs if timeoffuri not in assigned_enabled_timeoffs]
   to_disable = [timeoffuri for timeoffuri in assigned_enabled_timeoffs if timeoffuri not in available_timeoffs]
   return {"add": to_add, "disable": to_disable}

def build_timeoff_types_payload(dag_run):
   timeoff_updates = get_updated_timeoff_types(dag_run)
   payload = []

   if timeoff_updates["add"]:
       payload.append({
           "modificationOptionUri": "urn:replicon:collection-modification-option:add",
           "items": [{
               "timeOffType": {"uri": timeoff_uri, "name": null},
               "isTimeOffAllowedAgainstThisTimeOffType": true,
               "applyDefaultTimeOffTypePolicy": true,
               "defaultTimeOffTypePolicyEffectiveDate": rail.parse_date(dag_run.conf["user_details"]["hiredate"], EFFECTIVE_DATE_FORMAT_BAMBOOHR)
                   if dag_run.conf["user_details"]["hiredate"] else dag_run.conf["process_start_time"],
               "policySchedule": []
           } for timeoff_uri in timeoff_updates["add"]]
       })

   if timeoff_updates["disable"]:
       payload.append({
           "modificationOptionUri": "urn:replicon:collection-modification-option:add",
           "items": [{
               "timeOffType": {"uri": timeoff_uri, "name": null},
               "isTimeOffAllowedAgainstThisTimeOffType": false,
               "applyDefaultTimeOffTypePolicy": false,
               "defaultTimeOffTypePolicyEffectiveDate": null,
               "policySchedule": []
           } for timeoff_uri in timeoff_updates["disable"]]
       })

   return payload

def get_updated_project_role(dag_run):
    current_role = rail.result("get_user_assigned_role_from_replicon")
    project_role_uri = dag_run.conf["project_role_uri"]
    return (project_role_uri if project_role_uri and project_role_uri != (current_role[0]['schedule'][0]['projectRoles'][0]['projectRole']['uri']
        if current_role and current_role[0]['schedule'] else null) else null)

def get_updated_oefs(dag_run):
    updated_oefs = []
    for field_data in dag_run.conf["oef_fields"]:
        field_attr = field_data["field_attr"]
        oef_uri = dag_run.conf["oef_uris"].get(field_attr)
        user_value = dag_run.conf["user_details"].get(field_attr)
        
        if not (field_attr and oef_uri and user_value):
            continue
            
        # Get current value from Replicon
        current_value = None
        if field_data.get("oef_type") == "list":
            # For list OEFs, check tag.displayText
            current_value = rail.find_first_by_attr_and_get_attr(
                rail.result("get_user_details_from_replicon")["userDetails"]["extensionFieldValues"],
                "definition.uri", oef_uri, "tag.displayText"
            )
        else:
            # For text OEFs, check textValue
            current_value = rail.find_first_by_attr_and_get_attr(
                rail.result("get_user_details_from_replicon")["userDetails"]["extensionFieldValues"],
                "definition.uri", oef_uri, "textValue"
            )
            
        # Only add if value changed
        if user_value != current_value:
            oef_info = {
                "oefname": field_data["bamboohr_name"],
                "value": user_value,
                "uri": oef_uri,
                "oef_type": field_data.get("oef_type", "text"),
                "field_attr": field_attr
            }
            
            # Add tag URI for list OEFs
            if field_data.get("oef_type") == "list":
                tag_uri_key = f"{field_attr}_tag_uri"
                oef_info["tag_uri"] = dag_run.conf.get(tag_uri_key)
                
            updated_oefs.append(oef_info)
    
    return updated_oefs

def get_basic_user_details_update(dag_run):
    employment_range = rail.result("get_user_details_from_replicon")["userDetails"]["employmentDateRange"]
    start_date_json = employment_range.get("startDate")
    end_date_json = employment_range.get("endDate")
    
    # Get current dates from Replicon
    replicon_start_date = date(start_date_json["year"], start_date_json["month"], start_date_json["day"]
        ).strftime(EFFECTIVE_DATE_FORMAT_BAMBOOHR) if start_date_json else null
    replicon_end_date = date(end_date_json["year"], end_date_json["month"], end_date_json["day"]
        ).strftime(EFFECTIVE_DATE_FORMAT_BAMBOOHR) if end_date_json else null
    
    # Get dates from BambooHR
    bamboohr_start_date = dag_run.conf['user_details']['hiredate']
    bamboohr_end_date = dag_run.conf['user_details']['terminationdate']

    if bamboohr_end_date:
        enddate = bamboohr_end_date
    elif is_rehired_user(dag_run):
        enddate = null
    else:
        enddate = replicon_end_date

    # Check if either date has changed
    start_date_changed = bamboohr_start_date and bamboohr_start_date != replicon_start_date
    end_date_changed = enddate != replicon_end_date

    return {
        "startdate": bamboohr_start_date if bamboohr_start_date else replicon_start_date,
        "enddate": enddate,
        "has_changes": start_date_changed or end_date_changed,
        "replicon_start_date": replicon_start_date,
        "replicon_end_date": replicon_end_date
    }

def get_updated_display_name(dag_run):
    preferred_name = dag_run.conf["user_details"].get("preferredname")
    if preferred_name and preferred_name != rail.result("get_user_details_from_replicon")["userDetails"].get("customDisplayName"):
        return preferred_name
    elif not preferred_name:
        constructed_name = f'{dag_run.conf["user_details"]["lastname"]}, {dag_run.conf["user_details"]["firstname"]}'
        if constructed_name != rail.result("get_user_details_from_replicon")["userDetails"].get("customDisplayName"):
            return constructed_name
    return null

def get_updated_email(dag_run):
    """Check if email needs to be updated"""
    new_email = dag_run.conf["user_details"].get("workemail")
    current_email = rail.result("get_user_details_from_replicon")["userDetails"].get("emailAddress")

    if new_email and new_email != current_email:
        return new_email
    return null

def get_update_user_req(dag_run, required_employee_fields, licenses):
    current_date = dag_run.conf["process_start_time"]
    job_info_effective_date = rail.parse_date(dag_run.conf["user_details"]["jobinfoeffectivedate"], EFFECTIVE_DATE_FORMAT_BAMBOOHR)
    emp_info_effective_date = rail.parse_date(dag_run.conf["user_details"]["empstatuseffectivedate"], EFFECTIVE_DATE_FORMAT_BAMBOOHR)
    
    # Get current status information
    bamboo_status = dag_run.conf["user_details"]["status"].lower() if dag_run.conf["user_details"]["status"] else "active"
    current_is_enabled = rail.result("get_user_details_from_replicon")["userDetails"]["isEnabled"]
    
    # Get employment date details
    employment_dates = get_basic_user_details_update(dag_run)

    # Build modifications
    modifications = {
            "displayName": {"value": get_updated_display_name(dag_run)} if get_updated_display_name(dag_run) else null,
            "emailAddress": {"value": get_updated_email(dag_run)} if get_updated_email(dag_run) else null,
            "employmentDateRange": {
                "value": {
                    "startDate": rail.parse_date(employment_dates["startdate"], EFFECTIVE_DATE_FORMAT_BAMBOOHR) if employment_dates["startdate"] else null,
                    "endDate": rail.parse_date(employment_dates["enddate"], EFFECTIVE_DATE_FORMAT_BAMBOOHR) if employment_dates["enddate"] else null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            } if employment_dates["has_changes"] else null,
            "locationSchedule": [
                {
                    "dateRange": {
                        "startDate": job_info_effective_date if rail.result("get_current_group_membership").get("existinglocationuri") else null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": get_updated_location(dag_run),
                        "parentUri": null,
                        "name": null
                    }
                }
            ] if get_updated_location(dag_run) else [],
            "departmentGroupSchedule": [
                {
                    "dateRange": {
                        "startDate": job_info_effective_date if rail.result("get_current_group_membership").get("existingdepartmenturi") else null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": get_updated_department(dag_run),
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    }
                }
            ] if get_updated_department(dag_run) else [],
            "divisionSchedule": [
                {
                    "dateRange": {
                        "startDate": job_info_effective_date if rail.result("get_current_group_membership").get("existingdivisionuri") else null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": get_updated_division(dag_run),
                        "parentUri": null,
                        "name": null
                    }
                }
            ] if get_updated_division(dag_run) else [],
            "supervisorSchedule": [
                {
                    "dateRange": {
                        "startDate": job_info_effective_date if rail.result("get_supervisor_assignment_details") else null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": get_updated_supervisor(dag_run),
                        "loginName": null,
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                }
            ] if get_updated_supervisor(dag_run) else [],
            "holidayCalendarSchedule": [
                {
                    "dateRange": {
                        "startDate": current_date if rail.result("get_user_holiday_calendar") else null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": get_updated_holiday_calendar(dag_run),
                        "name": null
                    }
                }
            ] if get_updated_holiday_calendar(dag_run) else [],
            "extensionFields": [
                {
                    "value": oef_item
                } for oef_item in get_oef_details_to_update(dag_run)
            ] if get_updated_oefs(dag_run) else [],
            "projectRoleSchedule": [
                {
                    "dateRange": {
                        "startDate": current_date if rail.result("get_user_assigned_role_from_replicon") and rail.result("get_user_assigned_role_from_replicon")[0].get('schedule') else null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "projectRole": {
                            "uri": get_updated_project_role(dag_run),
                            "name": null
                        },
                        "isPrimary": true
                    }
                }
            ] if get_updated_project_role(dag_run) else [],
            "timeOffTypes": build_timeoff_types_payload(dag_run),
            "timesheetTemplate": null,
            "policySetsScheduleToApply": [
                {
                    "policyUri": "urn:replicon:policy:timesheet",
                    "schedule": [
                        {
                            "policySetUri": get_updated_timesheet(dag_run),
                            "effectiveDate": get_prior_saturday(current_date)
                        }
                    ]
                }
            ] if get_updated_timesheet(dag_run) else [],
            "payRuleSchedule": [
                {
                    "dateRange": {
                        "startDate": current_date if rail.result("get_user_details_from_replicon").get("payRuleScriptSchedule") else null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": get_updated_payrule(dag_run),
                        "name": null
                    }
                }
            ] if get_updated_payrule(dag_run) else [],
            "punchEntryPolicy": {
                "value": {
                    "uri": get_updated_punchentry_policy(dag_run),
                    "name": null
                }
            } if get_updated_punchentry_policy(dag_run) else null,
            "timeZone": {
                "value": {
                    "uri": get_updated_timezone(dag_run),
                    "IANAName": null
                }
            } if get_updated_timezone(dag_run) else null,
            "products": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                      {
                        "uri": null,
                        "name": license_name
                      }
                    ]
                } for license_name in licenses
            ] if is_rehired_user(dag_run) else [],
            "scheduleTypeSchedule": [
                {
                    "dateRange": {
                        "startDate": current_date if rail.result("get_user_details_from_replicon").get("schedulePolicies") else null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                        "officeSchedule": {
                            "officeScheduleUri": get_updated_schedule(dag_run),
                            "name": null
                        }
                    }
                }
            ] if get_updated_schedule(dag_run) else []
    }
    
    # Handle status-based modifications
    if bamboo_status in ["leave", "terminated", "inactive"] and current_is_enabled:
        # Disable login for both Leave and Terminated
        modifications["securitySettings"] = {
            "value": {
                "loginEnabled": {
                    "value": False
                }
            }
        }
        
        # For Terminated only: update end date and remove licenses
        if bamboo_status in ["terminated", "inactive"]:
            # Update end date if provided
            if dag_run.conf["user_details"]["terminationdate"]:
                # Always provide both dates to avoid removing existing dates
                existing_start = employment_dates["replicon_start_date"]
                bamboo_start = dag_run.conf["user_details"]["hiredate"]
                
                modifications["employmentDateRange"] = {
                    "value": {
                        "startDate": rail.parse_date(bamboo_start or existing_start, EFFECTIVE_DATE_FORMAT_BAMBOOHR) if (bamboo_start or existing_start) else null,
                        "endDate": rail.parse_date(dag_run.conf["user_details"]["terminationdate"], EFFECTIVE_DATE_FORMAT_BAMBOOHR),
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    }
                }
            
            # Remove licenses
            modifications["products"] = [{
                "modificationOptionUri": "urn:replicon:collection-modification-option:remove",
                "items": [{
                    "uri": null,
                    "name": license_name
                } for license_name in licenses]
            }]
    
    elif bamboo_status == "active" and not current_is_enabled:
        # Re-hire scenario: enable login
        modifications["securitySettings"] = {
            "value": {
                "loginEnabled": {
                    "value": True
                }
            }
        }
        # Note: Licenses are already handled by is_rehired_user logic above
    
    return {
        "target": {
            "uri": rail.result("get_user_details_from_replicon")["userDetails"]['uri'],
        },
        "modifications": modifications,
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_user_holiday_cal_payload(dag_run):
    effective_date = dag_run.conf["process_start_time"]
    return {
        "target": {
            "uri": rail.result("get_user_details_from_replicon")["userDetails"]['uri'],
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

def get_updated_logs(dag_run):
    employment_dates = get_basic_user_details_update(dag_run)
    groups_update_logs = []
    if get_updated_display_name(dag_run):
        groups_update_logs.append("Display Name updated")
    if get_updated_email(dag_run):
        groups_update_logs.append("Email updated")
    if get_updated_location(dag_run):
        groups_update_logs.append("Location updated")
    if get_updated_department(dag_run):
        groups_update_logs.append("Department updated")
    if get_updated_division(dag_run):
        groups_update_logs.append("Subsidiary updated")
    if get_updated_supervisor(dag_run):
        groups_update_logs.append("Supervisor updated")
    if get_updated_holiday_calendar(dag_run):
        groups_update_logs.append("Holiday Calendar updated")
    updated_oefs = get_updated_oefs(dag_run)
    if updated_oefs:
        groups_update_logs.extend([f"{oef['oefname']} updated" for oef in updated_oefs])
    if get_updated_project_role(dag_run):
        groups_update_logs.append("Project Role updated")
    if get_updated_timeoff_types(dag_run)["add"] or get_updated_timeoff_types(dag_run)["disable"]:
        groups_update_logs.append("Timeoff Types updated")
    if get_updated_timezone(dag_run):
        groups_update_logs.append("Time Zone updated")
    # Check if employment dates changed
    if employment_dates["has_changes"]:
        if employment_dates["startdate"] != employment_dates["replicon_start_date"]:
            groups_update_logs.append("User Start Date updated")
        if employment_dates["enddate"] != employment_dates["replicon_end_date"]:
            groups_update_logs.append("User End Date updated")
    if get_updated_timesheet(dag_run):
        groups_update_logs.append("Timesheet Template updated")
    if get_updated_punchentry_policy(dag_run):
        groups_update_logs.append("Time Punch Entry Policy updated")
    if get_updated_payrule(dag_run):
        groups_update_logs.append("Pay Rule updated")
    if get_updated_schedule(dag_run):
        groups_update_logs.append("Schedule Type updated")
    
    # Check status changes
    bamboo_status = dag_run.conf["user_details"]["status"].lower() if dag_run.conf["user_details"]["status"] else "active"
    current_is_enabled = rail.result("get_user_details_from_replicon")["userDetails"]["isEnabled"]
    
    if bamboo_status in ["leave", "terminated", "inactive"] and current_is_enabled:
        groups_update_logs.append(f"User status changed to {bamboo_status.title()} - Login disabled")
        if bamboo_status in ["terminated", "inactive"]:
            groups_update_logs.append("Licenses removed")
    elif bamboo_status == "active" and not current_is_enabled:
        groups_update_logs.append("User re-hired - Login enabled")
        if is_rehired_user(dag_run):
            groups_update_logs.append("Licenses re-assigned")

    # Check if payrule to be updated

    return groups_update_logs

def get_exception_logs(dag_run):
    groups_exception_logs = []
    if dag_run.conf["user_details"]["employeenumber"] == dag_run.conf["user_details"]["supervisorid"]:
        groups_exception_logs.append(f"User and Supervisor '{dag_run.conf['user_details']['supervisorid']}' ID's are same")
    elif dag_run.conf["user_details"]["supervisorid"] and not rail.result("get_supervisor_details") and dag_run.conf["user_details"]["supervisorid"] not in custom_methods.get_all_employee_numbers_from_payload(dag_run):
        groups_exception_logs.append(f"Supervisor '{dag_run.conf['user_details']['supervisorid']}' not present in Replicon")
    if not dag_run.conf["holiday_calendar_uri"]:
        groups_exception_logs.append(f"Holiday Calendar '{dag_run.conf['user_details']['holidaycalendar']}' not present in Replicon")
    if dag_run.conf["user_details"]["location"] and not dag_run.conf.get("location_uri"):
        groups_exception_logs.append(f"Location '{dag_run.conf['user_details']['location']}' not present in Replicon")
    if dag_run.conf["user_details"]["department"] and not dag_run.conf.get("department_uri"):
        groups_exception_logs.append(f"Department '{dag_run.conf['user_details']['department']}' not present in Replicon")
    if dag_run.conf["user_details"]["subsidiary"] and not dag_run.conf.get("division_uri"):
        groups_exception_logs.append(f"Division '{dag_run.conf['user_details']['subsidiary']}' not present in Replicon")
    missing_oefs = []
    missing_oef_tags = []
    for field_data in dag_run.conf["oef_fields"]:
        if field_data["field_attr"]:
            # Check if OEF URI is missing
            if not dag_run.conf["oef_uris"].get(field_data["field_attr"]):
                missing_oefs.append(field_data["replicon_name"])
            # For list OEFs, also check if tag URI is missing
            elif field_data.get("oef_type") == "list" and dag_run.conf["user_details"].get(field_data["field_attr"]):
                tag_uri_key = f"{field_data['field_attr']}_tag_uri"
                if not dag_run.conf.get(tag_uri_key):
                    missing_oef_tags.append(f"{field_data['replicon_name']} value '{dag_run.conf['user_details'][field_data['field_attr']]}'")
    if missing_oefs:
        groups_exception_logs.append(f"{', '.join(missing_oefs)} OEFs not present in Replicon")
    if missing_oef_tags:
        groups_exception_logs.append(f"OEF tag values not found in Replicon: {', '.join(missing_oef_tags)}")
    if not dag_run.conf["timesheet_template_uri"]:
        groups_exception_logs.append(
            f"Timesheet Template '{dag_run.conf['user_details']['timesheettype']}' not available in Replicon, hence assigned default timesheet template")
    if dag_run.conf["timeoff_types"]:
        missing_timeoff_types = [timeoff["name"] for timeoff in dag_run.conf["timeoff_types"] if timeoff["uri"] is null]
        if missing_timeoff_types:
            groups_exception_logs.append(f"Timeoff Types - {', '.join(missing_timeoff_types)} not available in Replicon")
    if not dag_run.conf["punch_entry_policy_uri"]:
        groups_exception_logs.append(f"Time Punch Entry Policy '{dag_run.conf['punch_entry_policy_name']}' not present in Replicon")
    if not dag_run.conf["project_role_uri"]:
        groups_exception_logs.append(f"Project Role '{dag_run.conf['user_details']['ratecode_oef']}' not present in Replicon")
    if not dag_run.conf["timezone_uri"]:
        groups_exception_logs.append(f"Time Zone '{dag_run.conf['user_details']['timezone']}' not present in Replicon. Hence default time zone is assigned")
    if not dag_run.conf["payrule_uri"]:
        groups_exception_logs.append(f"Pay Rule '{dag_run.conf['user_details']['payrule']}' not present in Replicon")
    if dag_run.conf["user_details"]["subsidiary"] and not dag_run.conf.get("office_schedule_uri"):
        groups_exception_logs.append(f"Office Schedule for '{dag_run.conf['user_details']['subsidiary']}' not present in Replicon")

    return groups_exception_logs

def get_update_user_error_notifications(dag_run):
    return ("User partially updated with errors - " + " | ".join(
        get_updated_logs(dag_run) + [details["displayText"]
            for details in rail.result("update_user_details")["errors"][0]["notifications"]]
                + get_exception_logs(dag_run)))

def get_updated_and_exception_log_message(dag_run):
    return ("User updated successfully" if not get_exception_logs(dag_run) and
        get_updated_logs(dag_run) else
            ("User partially updated - " + " | ".join(get_updated_logs(dag_run)
                + get_exception_logs(dag_run)))
                    if get_updated_logs(dag_run)
                        else ("User not updated - " + " | ".join(get_exception_logs(dag_run))
                            if get_exception_logs(dag_run) else "User not updated"))

def get_updated_and_exception_log_status(dag_run):
    return ("Success" if not get_exception_logs(dag_run) and
        get_updated_logs(dag_run) else ("Exception" if get_exception_logs(
            dag_run) else "Skipped"))

def get_put_oef_tags_payload(oef_uri, oef_tag_details, oef_field_name, new_oef_tags):
    return {
        "objectExtensionTagDefinition": {
            "uri": oef_uri,
            "name": null
        },
        "objectExtensionTags": [
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

def get_supervisor_assignment_payload(dag_run):
    """Get payload for supervisor assignment in supervisor assignment child DAG"""
    
    supervisor_uri = rail.result("get_supervisor_details")["userDetails"]["uri"]
    current_date = dag_run.conf["process_start_time"]
    
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
