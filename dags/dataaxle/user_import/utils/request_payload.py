import uuid
import rail

from dataaxle.user_import.utils import custom_methods


null = None

def create_job_title_payload(dag_run):
    return {
        "serviceCenter": null,
        "modifications": {
            "name": dag_run.conf["job_title"],
            "codeToApply": {
                "value": dag_run.conf["job_code"]
        },
        "descriptionToApply": null,
        "isEnabled": "true"
        },
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_all_custom_fields_drop_down_options(custom_field_uri):
    return {
         "customFieldUri": custom_field_uri
    }

def create_custom_fields_payload(dag_run):
    return {
        "customFieldUri": dag_run.conf["custom_field_uri"],
        "customFieldDropDownOptionUris": custom_methods.get_old_and_new_custom_fields()
    }

def build_employee_type_group_request_body():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:code",
            "urn:replicon:employee-type-group-list-column:employee-type-group"
        ],
        "sort": [],
        "filterExpression": null
    }

def update_name_request_payload(dag_run):
    return {
        "officeScheduleUri": rail.result("create_new_draft"),
        "name": dag_run.conf["standard_hours"]
    }

def apply_schedule_pattern_request_payload(dag_run):
    hours = int((float(dag_run.conf["standard_hours"])) / 5)
    return {
        "officeScheduleUri": rail.result("create_new_draft"),
        "pattern": {
            "startDayOfWeekUri": "urn:replicon:day-of-week:sunday",
        },
         "day1WorkDuration": {
            "hours": "0",
            "minutes": "0",
            "seconds": "0",
            "milliseconds": "0",
            "microseconds": "0"
        },
          "day2WorkDuration": {
            "hours": hours,
            "minutes": "0",
            "seconds": "0",
            "milliseconds": "0",
            "microseconds": "0"
            },
        "day3WorkDuration": {
            "hours": hours,
            "minutes": "0",
            "seconds": "0",
            "milliseconds": "0",
            "microseconds": "0"
        },
        "day4WorkDuration": {
            "hours": hours,
            "minutes": "0",
            "seconds": "0",
            "milliseconds": "0",
            "microseconds": "0"
        },
        "day5WorkDuration": {
            "hours": hours,
            "minutes": "0",
            "seconds": "0",
            "milliseconds": "0",
            "microseconds": "0"
        },
        "day6WorkDuration": {
            "hours": hours,
            "minutes": "0",
            "seconds": "0",
            "milliseconds": "0",
            "microseconds": "0"
        },
        "day7WorkDuration": {
            "hours": "0",
            "minutes": "0",
            "seconds": "0",
            "milliseconds": "0",
            "microseconds": "0"
        },
    }

def get_users_details_payload(empl_id):
    return {
        "users": [
            {
                "uri": null,
                "loginName": null,
                "employeeId": str(empl_id),
                "parameterCorrelationId": null,
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission",
    }


def build_bulk_get_users_payload():
    return {
        "users": [
            {
                "uri": null,
                "loginName": rail.result("extract_login_name"),
                "parameterCorrelationId": null,
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission",
    }


def _build_put_user2_payload(dag_run, permission_sets):
    """Shared PutUser2 payload builder. Pass permission_sets=[] for regular
    users and permission_sets=[{"uri": null, "name": "Supervisor"}] for supervisors."""
    login_name = rail.result("extract_login_name")
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": login_name,
                "parameterCorrelationId": null,
            },
            "firstname": dag_run.conf.get("first_name"),
            "lastname": dag_run.conf.get("last_name"),
            "emailAddress": dag_run.conf.get("email_id"),
            "employeeId": dag_run.conf.get("empl_id"),
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [],
            "workWeekStartDayUri": null,
            "employmentDateRange": {
                "startDate": dag_run.conf.get("hire_or_rehire"),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null,
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": login_name,
                "SSOName": login_name,
                "password": null,
            },
            "holidayCalendar": null,
            "timeOffPolicy": null,
            "permissionSets": permission_sets,
            "policySets": [{"uri": null, "name": "Time Off"}],
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": {
                "uri": null,
                "name": "Project Manager > Supervisor",
            },
            "expenseApprovalPath": null,
            "timeOffApprovalPath": null,
            "customFieldValues": [],
            "assignedActivities": [],
            "timeZone": null,
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [],
            "divisionSchedule": [],
            "costCenterSchedule": [],
            "serviceCenterSchedule": [],
            "departmentGroupSchedule": [],
            "employeeTypeGroupSchedule": [],
            "timesheetPeriodSchedule": [],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [],
            "displayNameParameter": null,
        }
    }


def build_create_user_payload(dag_run):
    return _build_put_user2_payload(dag_run, permission_sets=[])


def build_create_supervisor_user_payload(dag_run):
    return _build_put_user2_payload(dag_run, permission_sets=[{"uri": null, "name": "Supervisor"}])


def create_apply_department(dag_run):
    return {
        "userUri": (rail.result("create_user") or {}).get("uri"),
        "scheduleEntries": [
            {
                "departmentGroup": {
                    "uri": dag_run.conf.get("department"),
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null,
            }
        ]
    }


def build_combined_user_modifications_payload(dag_run):
    """
    Combines holiday calendar, timezone+location, division, employee type group,
    service center, and schedule policy into a single ApplyUserModifications3 call.
    Each modification key is included only when the corresponding conf field is present.
    Timezone and location are grouped together (only applied when both are present).
    """
    user_uri = (rail.result("create_user") or {}).get("uri")
    modifications = {}

    if dag_run.conf.get("holiday_calendar"):
        modifications["holidayCalendarToApply"] = {
            "holidayCalendar": {
                "uri": null,
                "name": dag_run.conf.get("holiday_calendar"),
            }
        }

    if dag_run.conf.get("timezone") and dag_run.conf.get("location_to_assign"):
        modifications["timezoneToApply"] = {
            "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
            "timezone": {
                "uri": null,
                "IANAName": dag_run.conf.get("timezone"),
            },
        }
        modifications["locationScheduleToApply"] = {
            "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
            "replacementLocationSchedule": [
                {
                    "location": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf.get("location_to_assign"),
                    },
                    "effectiveDate": null,
                }
            ],
            "updateLocationScheduleOverDateRange": null,
        }

    if dag_run.conf.get("division"):
        modifications["divisionScheduleToApply"] = {
            "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
            "replacementDivisionSchedule": [
                {
                    "division": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf.get("division"),
                    },
                    "effectiveDate": null,
                }
            ],
            "updateDivisionScheduleOverDateRange": null,
        }

    if dag_run.conf.get("hrly_or_salary"):
        modifications["employeeTypeGroupScheduleToApply"] = {
            "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
            "replacementEmployeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf.get("employee_type_group"),
                    },
                    "effectiveDate": null,
                }
            ],
            "updateEmployeeTypeGroupScheduleOverDateRange": null,
        }

    if dag_run.conf.get("job_title"):
        modifications["serviceCenterScheduleToApply"] = {
            "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
            "replacementServiceCenterSchedule": [
                {
                    "serviceCenter": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf.get("job_title"),
                    },
                    "effectiveDate": null,
                }
            ],
            "updateServiceCenterScheduleOverDateRange": null,
        }

    if dag_run.conf.get("standard_hours"):
        modifications["schedulePolicyToApply"] = {
            "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
            "replacementSchedule": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": null,
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf.get("standard_hours"),
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                    },
                    "effectiveDate": null,
                }
            ],
            "updateScheduleOverDateRange": null,
        }

    return {
        "user": {
            "uri": user_uri,
            "loginName": null,
            "parameterCorrelationId": null,
        },
        "modifications": modifications,
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
    }


def build_combined_supervisor_modifications_payload(dag_run):
    """
    Combines holiday calendar, timezone+location, division, employee type group,
    service center, and schedule policy into a single ApplyUserModifications3 call
    for the supervisor DAG.

    Schedule policy is applied when standard_hours is present OR when both
    payroll_dept_no and payroll_dept_name are present (Workato steps 24-25).
    Timezone and location are applied together only when both are present.
    """
    user_uri = (rail.result("create_user") or {}).get("uri")
    modifications = {}

    if dag_run.conf.get("holiday_calendar"):
        modifications["holidayCalendarToApply"] = {
            "holidayCalendar": {
                "uri": null,
                "name": dag_run.conf.get("holiday_calendar"),
            }
        }

    if dag_run.conf.get("timezone") and dag_run.conf.get("location_to_assign"):
        modifications["timezoneToApply"] = {
            "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
            "timezone": {
                "uri": null,
                "IANAName": dag_run.conf.get("timezone"),
            },
        }
        modifications["locationScheduleToApply"] = {
            "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
            "replacementLocationSchedule": [
                {
                    "location": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf.get("location_to_assign")
                    },
                    "effectiveDate": null,
                }
            ],
            "updateLocationScheduleOverDateRange": null
        }

    if dag_run.conf.get("division"):
        modifications["divisionScheduleToApply"] = {
            "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
            "replacementDivisionSchedule": [
                {
                    "division": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf.get("division")
                    },
                    "effectiveDate": null
                }
            ],
            "updateDivisionScheduleOverDateRange": null
        }

    if dag_run.conf.get("hrly_or_salary"):
        modifications["employeeTypeGroupScheduleToApply"] = {
            "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
            "replacementEmployeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": null,
                        "parent": null,
                        "name": "Salaried",
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ],
            "updateEmployeeTypeGroupScheduleOverDateRange": null
        }

    if dag_run.conf.get("job_title"):
        modifications["serviceCenterScheduleToApply"] = {
            "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
            "replacementServiceCenterSchedule": [
                {
                    "serviceCenter": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf.get("job_title")
                    },
                    "effectiveDate": null
                }
            ],
            "updateServiceCenterScheduleOverDateRange": null
        }

    if dag_run.conf.get("standard_hours") or (
        dag_run.conf.get("payroll_dept_no") and dag_run.conf.get("payroll_dept_name")
    ):
        modifications["schedulePolicyToApply"] = {
            "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
            "replacementSchedule": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": null,
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf.get("standard_hours")
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ],
            "updateScheduleOverDateRange": null
        }

    return {
        "user": {
            "uri": user_uri,
            "loginName": null,
            "parameterCorrelationId": null,
        },
        "modifications": modifications,
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
    }


def build_group_membership_modifications_payload(dag_run):
    """
    Builds a single ApplyUserModifications3 payload for an existing user update covering
    department, holiday calendar, timezone, location, and division.
    Each modification is included only when the conf value is present AND differs from
    the current value, matching the original per-task change-detection conditions.
    Uses update-schedule-over-date-range with effectiveDate = today_date.
    """
    modifications = {}
    membership = rail.result("get_user_group_membership")
    user_data = rail.result("get_user_data")

    current_department = (membership["departments"][0]["department"]["department"]["displayText"]
                          if membership.get("departments") else "")
    if (dag_run.conf.get("department") and
            dag_run.conf.get("company_name") != current_department):
        modifications["departmentGroupScheduleToApply"] = {
            "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementDepartmentGroupSchedule": [],
            "updateDepartmentGroupScheduleOverDateRange": {
                "replacementDepartmentGroupScheduleEntries": [
                    {
                        "departmentGroup": {
                            "uri": dag_run.conf.get("department"),
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null,
                        },
                        "effectiveDate": dag_run.conf.get("today_date"),
                    }
                ],
                "endDate": null,
            },
        }

    if (dag_run.conf.get("holiday_calendar") and
            dag_run.conf.get("holiday_calendar") != (user_data[0]["holidayCalendar"]["name"] if user_data[0].get("holidayCalendar") else "")):
        modifications["holidayCalendarToApply"] = {
            "holidayCalendar": {
                "uri": null,
                "name": dag_run.conf.get("holiday_calendar"),
            }
        }

    if (dag_run.conf.get("timezone") and
            dag_run.conf.get("timezone") != (user_data[0]["timeZone"]["ianaName"] if user_data[0].get("timeZone") else "")):
        modifications["timezoneToApply"] = {
            "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
            "timezone": {
                "uri": null,
                "IANAName": dag_run.conf.get("timezone"),
            },
        }

    current_location = (membership["locations"][0]["location"]["location"]["displayText"]
                        if len(membership["locations"]) > 0 else "")
    if (dag_run.conf.get("location_to_assign") and
            dag_run.conf.get("location_to_assign") != current_location):
        modifications["locationScheduleToApply"] = {
            "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementLocationSchedule": [],
            "updateLocationScheduleOverDateRange": {
                "replacementLocationScheduleEntries": [
                    {
                        "location": {
                            "uri": null,
                            "parentUri": null,
                            "name": dag_run.conf.get("location_to_assign"),
                        },
                        "effectiveDate": dag_run.conf.get("today_date"),
                    }
                ],
                "endDate": null,
            },
        }

    current_division = (membership["divisions"][0]["division"]["division"]["displayText"]
                        if len(membership["divisions"]) > 0 else "")
    if (dag_run.conf.get("division") and
            dag_run.conf.get("division") != current_division):
        modifications["divisionScheduleToApply"] = {
            "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementDivisionSchedule": [],
            "updateDivisionScheduleOverDateRange": {
                "replacementDivisionScheduleEntries": [
                    {
                        "division": {
                            "uri": null,
                            "parentUri": null,
                            "name": dag_run.conf.get("division"),
                        },
                        "effectiveDate": dag_run.conf.get("today_date"),
                    }
                ],
                "endDate": null,
            },
        }

    return {
        "user": {
            "uri": dag_run.conf.get("user_uri"),
            "loginName": null,
            "parameterCorrelationId": null,
        },
        "modifications": modifications,
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
    }


def apply_custom_field_drop_down_value_payload(user_uri, custom_field_uri, custom_field_drop_down_option_uri):
    return {
        "objectUri": user_uri,
        "customFieldUri": custom_field_uri,
        "customFieldDropDownOptionUri": custom_field_drop_down_option_uri
    }

def apply_hourly_payroll_currency_payload(dag_run):
    return {
        "userUri": (rail.result("create_user") or {}).get("uri"),
        "schedule": {
            "initialHourlyRate": {
                "amount": "0",
                "currency": {
                    "uri": null,
                    "name": null,
                    "symbol": dag_run.conf.get("hourly_payroll_currency")
                }
            },
            "scheduleEntries": []
        }
    }


def update_user_specific_billing_rate_amount_payload(dag_run):
    return {
        "userUri": (rail.result("create_user") or {}).get("uri"),
        "rate": {
            "amount": "0",
            "currencyUri": dag_run.conf.get("currency_uri")
        }
    }


def apply_cost_rate_payload(dag_run):
    return {
        "userUri": (rail.result("create_user") or {}).get("uri"),
        "hourlyRate": {
            "amount": "0",
            "currencyUri": dag_run.conf.get("currency_uri")
        },
        "dateRange": null
    }


def assign_user_permission_payload(user_uri, permission_uri):
    return {
        "userUri": user_uri,
        "permissionSetUri": permission_uri,
    }


def assign_supervisor_payload(user_uri, supervisor_uri):
    return {
        "userUri": user_uri,
        "initialSupervisorUri": supervisor_uri,
        "scheduleEntries": []
    }


def get_user_data_by_uri_payload(user_uri):
    return {
        "users": [
            {
                "uri": user_uri,
                "loginName": null,
                "parameterCorrelationId": null,
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission",
    }


def get_user_group_membership_payload(user_uri):
    return {
        "userUri": user_uri,
        "dateRange": null,
    }


def update_employment_date_range_payload(user_uri, start_date):
    return {
        "userUri": user_uri,
        "dateRange": {
            "startDate": start_date,
            "endDate": null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null,
        },
    }


def update_login_and_sso_name_payload(user_uri, login_name):
    return {
        "user": {
            "uri": user_uri,
            "loginName": null,
            "parameterCorrelationId": null,
        },
        "modifications": {
            "securitySettingsToApply": {
                "loginEnabled": "true",
                "forcePasswordChange": "false",
                "loginName": login_name,
                "ssoName": login_name,
                "password": null,
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "emailMFAResendVerificationEmail": "false",
                "emailMFATryAddMethodFromUsersEmail": "false",
                "isMFAMethodRequired": "false",
                "userSSONameModificationOptionUri": null,
                "clearIsLockedOut": "false",
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
    }


def update_user_details(dag_run):
    existing_user_data = rail.result('get_user_data')
    first_name = None if dag_run.conf.get("first_name") == existing_user_data[0]["userDetails"]["firstName"] else dag_run.conf.get("first_name")
    last_name = None if dag_run.conf.get("last_name") == existing_user_data[0]["userDetails"]["lastName"] else dag_run.conf.get("last_name")
    email_address = existing_user_data[0]["userDetails"]["emailAddress"] if existing_user_data[0]["userDetails"]["emailAddress"] and dag_run.conf.get("email_id") == existing_user_data[0]["userDetails"]["emailAddress"] else dag_run.conf.get("email_id")

    return {
        "user": {
            "uri": dag_run.conf.get("user_uri")
        },
        "modifications": {
            "userDetailsToApply": {
                "firstName": first_name,
                "lastName": last_name,
                "emailAddress": {
                    "emailAddress": email_address
                },
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
    }


def update_employee_type_group_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf.get("user_uri"),
            "loginName": null,
            "parameterCorrelationId": null,
        },
        "modifications": {
            "employeeTypeGroupScheduleToApply": {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {
                                "uri": null,
                                "parentUri": null,
                                "name": dag_run.conf.get("employee_type_group")
                            },
                            "effectiveDate": dag_run.conf.get("today_date"),
                        }
                    ],
                    "endDate": null,
                },
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
    }


def update_service_center_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf.get("user_uri"),
            "loginName": null,
            "parameterCorrelationId": null,
        },
        "modifications": {
            "serviceCenterScheduleToApply": {
                "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementServiceCenterSchedule": [],
                "updateServiceCenterScheduleOverDateRange": {
                    "replacementServiceCenterScheduleEntries": [
                        {
                            "serviceCenter": {
                                "uri": null,
                                "parentUri": null,
                                "name": dag_run.conf.get("job_title")
                            },
                            "effectiveDate": dag_run.conf.get("today_date"),
                        }
                    ],
                    "endDate": null,
                },
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
    }


def update_schedule_policy_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf.get("user_uri"),
            "loginName": null,
            "parameterCorrelationId": null,
        },
        "modifications": {
            "schedulePolicyToApply": {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementSchedule": [],
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": null,
                                "officeSchedule": {
                                    "officeScheduleUri": null,
                                    "name": dag_run.conf.get("standard_hours")
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                            },
                            "effectiveDate": dag_run.conf.get("today_date"),
                        }
                    ],
                    "endDate": null,
                },
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
    }


def get_current_supervisor_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:supervisor",
            "urn:replicon:user-list-column:user",
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:user-list-filter:user",
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": dag_run.conf.get("user_uri"),
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
                },
                "filterDefinitionUri": null,
            },
            "value": null,
            "filterDefinitionUri": null,
        },
    }


def update_supervisor_payload(user_uri, supervisor_uri, start_date):
    return {
        "userUri": user_uri,
        "supervisorUri": supervisor_uri,
        "dateRange": {
            "startDate": start_date,
            "endDate": null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null,
        },
    }