from datetime import datetime
import uuid
import pendulum
import rail

null = None
true = "true"
false = "false"
DATE_FORMAT = "%Y/%m/%d"
DEFAULT_TERMINATION_DATE = "9999/12/31"


def get_timesheet_period_payload():
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:timesheet-period-list-column:timesheet-period"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:timesheet-period-list-filter:enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": true,
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

def get_user_holiday_cal_payload(dag_run):
    effective_date = dag_run.conf["process_start_time"]
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

def get_create_new_user_payload(dag_run, config):
    return {
        "target": null,
        "template": {
            "templateTarget": null
        },
        "modifications": {
            "firstName": {
                "value": dag_run.conf["first_name"]
            },
            "lastName": {
                "value": dag_run.conf["last_name"]
            },
            "loginName": {
                "value": dag_run.conf["employee_id"]
            },
            "displayName": null,
            "emailAddress": {
                "value": dag_run.conf["employee_email"]
            },
            "employeeId": {
                "value": dag_run.conf["employee_id"]
            },
            "employmentDateRange": {
                "value": {
                    "startDate": rail.parse_date(dag_run.conf["start_date"], DATE_FORMAT),
                    "endDate": rail.parse_date(dag_run.conf["termination_date"], DATE_FORMAT)
                        if dag_run.conf["termination_date"] and dag_run.conf["termination_date"] != DEFAULT_TERMINATION_DATE else null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": true if dag_run.conf["employee_status"] == "Enabled" else false
                    },
                    "forcePasswordChange": null,
                    "ssoName": {
                        "value": dag_run.conf["employee_id"]
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
                    "name": "Client Representative"
                }
            },
            "timeEntryApprovalPath": {
                "value": {
                    "uri": null,
                    "name": "Project Manager"
                }
            },
            "workAuthorizationApprovalPath": null,
            "timeoffApprovalPath": null,
            "timeOffBalancePayoutApprovalPath": null,
            "defaultActivity": null,
            "expenseApprovalPath": null,
            "timeZone": null,
            "workWeekStartDay": {
                "value": {
                    "uri": "urn:replicon:day-of-week:monday"
                }
            },
            "defaultBillingRate": null,
            "userPreferences": null,
            "formattings": null,
            "notificationPreferences": null,
            "timesheetTemplate": {
                "value": {
                    "uri": null,
                    "name": config.location_wise_data_mapper[dag_run.conf["location_code"]]["timesheet_template"]
                }
            } if dag_run.conf["location_code"] in (config.location_wise_data_mapper).keys() else null,
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
            "holidayCalendar": null,
            "extensionFields": [
                {
                    "value": {
                        "definition": {
                            "uri": dag_run.conf["workforceid_oef_uri"],
                            "name": null
                        },
                        "textValue": dag_run.conf["workforce_id"]
                    }
                }
            ],
            "customFields": [],
            "products": [],
            "skills": [],
            "activities": [],
            "policySets": [],
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "uri": null,
                                "name": config.USER_PERMISSION
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
                                "uri": timeoff_type["uri"],
                                "name": null
                            },
                            "isTimeOffAllowedAgainstThisTimeOffType": true,
                            "applyDefaultTimeOffTypePolicy": true,
                            "defaultTimeOffTypePolicyEffectiveDate": rail.parse_date(dag_run.conf["start_date"], DATE_FORMAT),
                            "policySchedule": []
                        }
                        for timeoff_type in dag_run.conf["timeoff_types"]
                    ]
                }
            ] if dag_run.conf["location_code"] in (config.location_wise_data_mapper).keys() else [],
            "locationSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf["location_name"]
                    }
                }
            ] if rail.result("get_required_location") else [],
            "divisionSchedule": [],
            "costCenterSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf["costcenter_name"]
                    }
                }
            ] if rail.result("get_required_costcenter") else [],
            "serviceCenterSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf["company_code_name"]
                    }
                }
            ] if rail.result("get_required_servicecenter") else [],
            "departmentGroupSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "parent": {
                            "uri": null,
                            "parent": null,
                            "name": "BearingPoint",
                            "parameterCorrelationId": null
                        },
                        "name": f'{dag_run.conf["department_name"]}-{dag_run.conf["department_code"]}',
                        "parameterCorrelationId": null
                    }
                }
            ] if rail.result("get_required_department") else [],
            "employeeTypeGroupSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "parent": null,
                        "name": dag_run.conf["employee_type_name"],
                        "parameterCorrelationId": null
                    }
                }
            ] if rail.result("get_required_employeetype") else [],
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
                        "loginName": dag_run.conf["supervisor"],
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                }
            ] if dag_run.conf["employee_id"] != dag_run.conf["supervisor"] and rail.result("get_supervisor_details") else [],
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
                        "name": "Weekly without crossing months"
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
            ] if dag_run.conf["holiday_calendar_uri"] else null,
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
                            "officeScheduleUri": null,
                            "name": dag_run.conf["work_schedule"]
                        }
                    }
                }
            ] if dag_run.conf["work_schedule_uri"] else [],
            "payRuleSchedule": [],
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

def put_timeoff_assignment_payload(dag_run):
    return {
        'userUri': rail.result("get_user_details")["userDetails"]['uri'],
        'timeOffTypeUris': [timeoff_type["uri"] for timeoff_type in dag_run.conf["timeoff_types"]]
    }


def is_rehired_user(dag_run):
    return "rehired" if (dag_run.conf["employee_status"] == "Enabled" and
        not rail.result("get_user_details")["securityConfiguration"]["isLoginEnabled"]) else null


def get_date_in_format(date_str, date_format):
    return datetime.strptime(date_str, date_format)


def get_basic_user_details_update(dag_run, location_wise_data_mapper):
    first_name = null
    last_name = null
    basic_details_logs = []
    email_id = null
    status = null
    update_timeofftypes = False
    update_timeoff_types_log = null
    replicon_start_date = rail.result("get_user_details")["userDetails"]["employmentDateRange"]["startDate"]
    existing_start_date = f'{replicon_start_date["year"]}/{replicon_start_date["month"]}/{replicon_start_date["day"]}' if replicon_start_date else null
    replicon_end_date = rail.result("get_user_details")["userDetails"]["employmentDateRange"]["endDate"]
    existing_end_date = f'{replicon_end_date["year"]}/{replicon_end_date["month"]}/{replicon_end_date["day"]}' if replicon_end_date else null
    start_date = rail.parse_date(existing_start_date, DATE_FORMAT) if existing_start_date else null
    end_date = rail.parse_date(existing_end_date, DATE_FORMAT) if existing_end_date else null
    start_date_changed = False
    end_date_changed = False
    if dag_run.conf["first_name"] != rail.result("get_user_details")["userDetails"]["firstName"]:
        first_name = dag_run.conf["first_name"]
        basic_details_logs.append("First name updated")
    if dag_run.conf["last_name"] != rail.result("get_user_details")["userDetails"]["lastName"]:
        last_name = dag_run.conf["last_name"]
        basic_details_logs.append("Last name updated")
    if dag_run.conf["employee_email"] != rail.result("get_user_details")["userDetails"]["emailAddress"]:
        email_id = dag_run.conf["employee_email"]
        basic_details_logs.append("Email updated")
    if not existing_start_date:
        start_date = rail.parse_date(dag_run.conf["start_date"], DATE_FORMAT)
        start_date_changed = True
        basic_details_logs.append("Start date updated")
    if existing_start_date and get_date_in_format(dag_run.conf["start_date"], DATE_FORMAT) != get_date_in_format(existing_start_date, DATE_FORMAT):
        start_date = rail.parse_date(dag_run.conf["start_date"], DATE_FORMAT)
        start_date_changed = True
        basic_details_logs.append("Start date updated")
    if not existing_end_date and dag_run.conf["termination_date"] and dag_run.conf["termination_date"] != DEFAULT_TERMINATION_DATE:
        end_date = rail.parse_date(dag_run.conf["termination_date"], DATE_FORMAT)
        end_date_changed = True
        basic_details_logs.append("End date updated")
    if (dag_run.conf["termination_date"] and dag_run.conf["termination_date"] != DEFAULT_TERMINATION_DATE and existing_end_date
            and get_date_in_format(dag_run.conf["termination_date"], DATE_FORMAT) != get_date_in_format(existing_end_date, DATE_FORMAT)):
        end_date = rail.parse_date(dag_run.conf["termination_date"], DATE_FORMAT)
        end_date_changed = True
        basic_details_logs.append("End date updated")
    if get_updated_location(dag_run):
        update_timeoff_types_log = "Timeoff types assigned"
    if dag_run.conf["employee_status"] == "Disabled" and rail.result("get_user_details")["securityConfiguration"]["isLoginEnabled"]:
        if dag_run.conf["termination_date"]:
            status = false
            basic_details_logs.append("Login status disabled")
    elif is_rehired_user(dag_run) == "rehired":
        status = true
        end_date = null
        end_date_changed = True
        update_timeofftypes = True
        update_timeoff_types_log = "Timeoff types assigned"
        basic_details_logs.append("Login status enabled")
    if update_timeoff_types_log:
        basic_details_logs.append(update_timeoff_types_log)
    basic_details = {
        "firstName": {
            "value": first_name
        } if first_name else null,
        "lastName": {
            "value": last_name
        } if last_name else null,
        "emailAddress": {
            "value": email_id
        } if email_id else null,
        "securitySettings": {
            "value": {
                "loginEnabled": {
                    "value": status
                }
            }
        } if status else null,
        "employmentDateRange": {
            "value": {
                "startDate": start_date,
                "endDate": end_date,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            }
        } if start_date_changed or end_date_changed else null,
        "timeOffTypes": [
            {
                "modificationOptionUri": "urn:replicon:collection-modification-option:replace",
                "items": [
                    {
                        "timeOffType": {
                            "uri": timeoff_type["uri"],
                            "name": null
                        },
                        "isTimeOffAllowedAgainstThisTimeOffType": true,
                        "applyDefaultTimeOffTypePolicy": true,
                        "defaultTimeOffTypePolicyEffectiveDate": rail.parse_date(dag_run.conf["start_date"], DATE_FORMAT),
                        "policySchedule": []
                    }
                    for timeoff_type in dag_run.conf["timeoff_types"]
                ]
            }
        ] if update_timeofftypes and dag_run.conf["location_code"] in (location_wise_data_mapper).keys() else [],
    }

    return basic_details, basic_details_logs


def get_updated_logs(dag_run, config):
    basic_details_logs = get_basic_user_details_update(dag_run, config.location_wise_data_mapper)[1]
    groups_update_logs = []
    if get_updated_location(dag_run):
        groups_update_logs.append("Location updated")
    if get_updated_department(dag_run):
        groups_update_logs.append("Department updated")
    if get_updated_costcenter(dag_run):
        groups_update_logs.append("Cost Center updated")
    if get_updated_employeetype(dag_run):
        groups_update_logs.append("Employee Type updated")
    if get_updated_servicecenter(dag_run):
        groups_update_logs.append("Service Center updated")
    if get_updated_supervisor(dag_run):
        groups_update_logs.append("Supervisor updated")
    if dag_run.conf["work_schedule_uri"] and get_current_schedule(
        rail.result("get_user_details")["schedulePolicies"], config.time_zone) != dag_run.conf["work_schedule_uri"]:
        groups_update_logs.append("Schedule type updated")
    if get_updated_holiday_calendar(dag_run):
        groups_update_logs.append("Holiday calendar updated")
    if get_updated_workforceid(dag_run):
        groups_update_logs.append("WorkforceID updated")

    return basic_details_logs + groups_update_logs

def get_exception_logs(dag_run, location_wise_data_mapper):
    groups_exception_logs = []
    if not dag_run.conf["work_schedule_uri"]:
        groups_exception_logs.append(f"Work schedule \'{dag_run.conf['work_schedule']}\' not present in replicon")
    if dag_run.conf["employee_id"] == dag_run.conf["supervisor"]:
        groups_exception_logs.append(f"User and Supervisor \'{dag_run.conf['supervisor']}\' ID's are same")
    elif not rail.result("get_supervisor_details"):
        groups_exception_logs.append(f"Supervisor \'{dag_run.conf['supervisor']}\' not present in replicon")
    if not dag_run.conf["holiday_calendar_uri"]:
        groups_exception_logs.append(f"Holiday calendar \'{dag_run.conf['holiday_calendar']}\' not present in replicon")
    if not dag_run.conf["workforceid_oef_uri"]:
        groups_exception_logs.append("Workforce ID OEF not present in replicon")
    if (dag_run.conf["employee_status"] == "Disabled" and rail.result("get_user_details") and 
        rail.result("get_user_details")["securityConfiguration"]["isLoginEnabled"]):
        if not dag_run.conf["termination_date"]:
            groups_exception_logs.append("Employee status is 'Disabled' with no termination date")
    if dag_run.conf["location_code"] not in location_wise_data_mapper.keys():
        groups_exception_logs.append(f"Timeoff types for Location code \'{dag_run.conf['location_code']}\' not available in pre-defined mapper")
        groups_exception_logs.append(f"Timesheet template for Location code \'{dag_run.conf['location_code']}\' not available in pre-defined mapper")
    if dag_run.conf["location_code"] in location_wise_data_mapper.keys():
        location_template_name = location_wise_data_mapper[dag_run.conf["location_code"]]["timesheet_template"] if dag_run.conf["location_code"] in location_wise_data_mapper else null
        if location_template_name and not rail.find_first_by_attr_and_get_attr(dag_run.conf["timesheet_template"], "timesheet_template_name", location_template_name, "uri"):
            groups_exception_logs.append(f"Timesheet template \'{location_template_name}\' not present in replicon")
    return groups_exception_logs

def get_updated_location(dag_run):
    return dag_run.conf["location_name"] if (rail.result("get_required_location") and
        dag_run.conf["location_name"] != rail.result("get_current_group_membership")["existinglocationname"]) else null

def get_updated_timesheet_template(dag_run, location_wise_data_mapper):
    if not dag_run.conf.get("timesheet_template"):
        return null
    
    # Find the template URI based on location code mapping
    location_template_name = location_wise_data_mapper[dag_run.conf["location_code"]]["timesheet_template"] if dag_run.conf["location_code"] in location_wise_data_mapper else null
    if not location_template_name:
        return null
    
    # Find the matching template from the list
    template_uri = rail.find_first_by_attr_and_get_attr(
        dag_run.conf["timesheet_template"], 
        "timesheet_template_name", 
        location_template_name, 
        "uri"
    )
    
    # Check if current user's template is different
    current_template_uri = (rail.result("get_user_details")["timesheetTemplate"]["uri"] 
                          if rail.result("get_user_details") and rail.result("get_user_details")["timesheetTemplate"] else null)
    
    return template_uri if (template_uri and template_uri != current_template_uri) else null

def get_updated_department(dag_run):
    return (f'{dag_run.conf["department_name"]}-{dag_run.conf["department_code"]}'
            if rail.result("get_required_department") and (f'{dag_run.conf["department_name"]}-{dag_run.conf["department_code"]}'
            	!= rail.result("get_current_group_membership")["existingdepartmentname"]) else null)


def get_updated_costcenter(dag_run):
    return dag_run.conf["costcenter_name"] if (rail.result("get_required_costcenter") and
        dag_run.conf["costcenter_name"] != rail.result("get_current_group_membership")["existingcostcentername"]) else null


def get_updated_employeetype(dag_run):
    return dag_run.conf["employee_type_name"] if (rail.result("get_required_employeetype") and
        dag_run.conf["employee_type_name"] != rail.result("get_current_group_membership")["existingemployeetypename"]) else null


def get_updated_servicecenter(dag_run):
    return dag_run.conf["company_code_name"] if (rail.result("get_required_servicecenter") and
        dag_run.conf["company_code_name"] != rail.result("get_current_group_membership")["existingservicecentername"]) else null


def get_updated_supervisor(dag_run):
    return (dag_run.conf["supervisor"] if (dag_run.conf["supervisor"] != dag_run.conf["employee_id"] and rail.result("get_supervisor_details")
        and dag_run.conf["supervisor"] != rail.result("get_supervisor_assignment_details", "supervisor").get("user", {}).get("loginName", "")) else null)


def get_updated_holiday_calendar(dag_run):
    return (dag_run.conf["holiday_calendar_uri"] if dag_run.conf["holiday_calendar_uri"]
        and dag_run.conf["holiday_calendar_uri"] != (rail.result("get_user_holiday_calendar")["uri"]
            if rail.result("get_user_holiday_calendar") else null)
    		    else null)


def get_updated_workforceid(dag_run):
    return dag_run.conf["workforce_id"] if dag_run.conf["workforce_id"] != rail.find_first_by_attr_and_get_attr(
        rail.result("get_user_details")["userDetails"]["extensionFieldValues"], "definition.uri", dag_run.conf["workforceid_oef_uri"], "textValue"
    ) else null


def get_current_schedule(data, timezone):
    if not data and len(data) == 0:
        return None
    current_schedule = list(filter(lambda x: datetime(
        **x['effectiveDate']) if x['effectiveDate'] else datetime.min.date() <= pendulum.now(timezone).date(), data))
    return null if len(current_schedule) == 0 else current_schedule[-1]["officeSchedule"]["uri"]


def get_update_user_req(dag_run, time_zone, location_wise_data_mapper):
    current_date = dag_run.conf["process_start_time"]
    return {
        "target": {
            "uri": rail.result("get_user_details")["userDetails"]['uri'],
        },
        "modifications": {
            **get_basic_user_details_update(dag_run, location_wise_data_mapper)[0],
            "policySetsScheduleToApply": [
                {
                    "policyUri": "urn:replicon:policy:timesheet",
                    "schedule": [
                        {
                            "policySetUri": get_updated_timesheet_template(dag_run, location_wise_data_mapper),
                            "effectiveDate": current_date
                        }
                    ]
                }
            ] if get_updated_timesheet_template(dag_run, location_wise_data_mapper) and get_updated_location(dag_run) else [],
            "locationSchedule": [
                {
                    "dateRange": {
                        "startDate": current_date,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": get_updated_location(dag_run)
                    }
                }
            ] if get_updated_location(dag_run) else [],
            "departmentGroupSchedule": [
                {
                    "dateRange": {
                        "startDate": current_date,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "parent": {
                            "uri": null,
                            "parent": null,
                            "name": "BearingPoint",
                            "parameterCorrelationId": null
                        },
                        "name": get_updated_department(dag_run),
                        "parameterCorrelationId": null
                    }
                }
            ] if get_updated_department(dag_run) else [],
            "costCenterSchedule": [
                {
                    "dateRange": {
                        "startDate": current_date,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": get_updated_costcenter(dag_run)
                    }
                }
            ] if get_updated_costcenter(dag_run) else [],
            "employeeTypeGroupSchedule": [
                {
                    "dateRange": {
                        "startDate": current_date,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "parent": null,
                        "name": get_updated_employeetype(dag_run),
                        "parameterCorrelationId": null
                    }
                }
            ] if get_updated_employeetype(dag_run) else [],
            "serviceCenterSchedule": [
                {
                    "dateRange": {
                        "startDate": current_date,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": get_updated_servicecenter(dag_run)
                    }
                }
            ] if get_updated_servicecenter(dag_run) else [],
            "supervisorSchedule": [
                {
                    "dateRange": {
                        "startDate": current_date if rail.result("get_supervisor_assignment_details", "supervisor") else null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "loginName": get_updated_supervisor(dag_run),
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                }
            ] if get_updated_supervisor(dag_run) else [],
            "scheduleTypeSchedule": [
                {
                    "dateRange": {
                        "startDate": current_date if get_current_schedule(rail.result("get_user_details")["schedulePolicies"], time_zone) else null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf["work_schedule"]
                        }
                    }
                }
            ] if dag_run.conf["work_schedule_uri"] and get_current_schedule(rail.result("get_user_details")["schedulePolicies"],
                time_zone) != dag_run.conf["work_schedule_uri"] else [],
            "holidayCalendar": null,
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
                    "value": {
                        "definition": {
                            "uri": dag_run.conf["workforceid_oef_uri"],
                            "name": null
                        },
                        "textValue": get_updated_workforceid(dag_run)
                    }
                }
            ] if get_updated_workforceid(dag_run) else []
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }
