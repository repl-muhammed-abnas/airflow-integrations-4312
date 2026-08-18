from sigroup.user_import.utils import custom_methods
import rail
null = None
MMDDYYY="%m/%d/%Y"

def get_policy_sets(dag_run):
    policy_sets = []

    if dag_run.conf["timesheettemplate"]:
        policy_sets.append({
        "uri": dag_run.conf["timesheettemplate"],
        "name": null
      })
    if dag_run.conf["timeofftemplate"]:
        policy_sets.append({
        "uri": dag_run.conf["timeofftemplate"],
        "name": null
      })
    if dag_run.conf["punchentrypolicy"]:
        policy_sets.append({
        "uri": dag_run.conf["punchentrypolicy"],
        "name": null
      })
    return policy_sets


def get_custom_field_values(dag_run):
    custom_field_values = []
    custom_fields_text = ["action", "status", "shift", "cloudpay_paycode",]
    custom_field_dropdown = ["locationstate", "locationcity", "employee_type",
                             "manufacturing", "coefficientlevel", "elderlyallowance",
                             "apprentice", "timecode", "cbaappendix", "istariffemployee",
                             "tariffclassification", "stepinformation", "workleader"]
    custom_fields_dates = ["actioneffectivedate",
                           "workinglifestartdate", "ptoservicedate"]

    for i in custom_fields_text:
        if dag_run.conf[i]:
            custom_field_values.append({
                "customField": {
                    "uri": dag_run.conf[i+"uri"],
                    "name": null,
                    "groupUri": null
                },
                "text": dag_run.conf[i],
                "date": null,
                "dropDownOption": null,
                "number": null
            })
    for i in custom_field_dropdown:
        if dag_run.conf[i]:
            custom_field_values.append({
                "customField": {
                    "uri": dag_run.conf[i+"uri"],
                    "name": null,
                    "groupUri": null
                },
                "text": null,
                "date": null,
                "dropDownOption": {
                    "uri": dag_run.conf[i]
                },
                "number": null
            })
    for i in custom_fields_dates:
        if dag_run.conf[i]:
            custom_field_values.append({
                "customField": {
                    "uri": dag_run.conf[i+"uri"],
                    "name": null,
                    "groupUri": null
                },
                "text": null,
                "date": rail.parse_date(dag_run.conf[i], MMDDYYY),
                "dropDownOption": null,
                "number": null
            })
    if dag_run.conf["fte"]:
        custom_field_values.append({
            "customField": {
                "uri": dag_run.conf["fteuri"],
                "name": null,
                "groupUri": null
            },
            "text": null,
            "date": null,
            "dropDownOption": null,
            "number": dag_run.conf["fte"]
        })
    return custom_field_values


def get_activity_list(dag_run):
    return list(map(lambda i: {"name": i},
                    custom_methods.get_valid_activity_names(dag_run)))


def get_payrule_schedule(dag_run):
    payrule_value = ""
    if dag_run.conf["businessunitcodeforpayrule"] == "01":
        if dag_run.conf["employee_type"] == "H":
            payrule_value = rail.find_first_by_attr_and_get_attr(
                "Shift",
                dag_run.conf["shift"],
                "employee_type",
                dag_run.conf["employee_type"]
            )
        else:
            payrule_value = rail.find_first_by_attr_and_get_attr(
                "Location",
                dag_run.conf["businessunitcodeforpayrule"],
                "employee_type",
                dag_run.conf["employee_type"]
            )
    if payrule_value and payrule_value["Value"]:
        return [{
            "payRuleScript": {
                "uri": null,
                "name": payrule_value["Value"]
            },
            "effectiveDate": null
        }]
    if dag_run.conf["payrule"]:
        return [{
            "payRuleScript": {
                "uri":dag_run.conf["payrule"],
                "name": null
            },
            "effectiveDate": null
        }]
    return null


def get_schedule_policy(dag_run):
    if dag_run.conf["scheduletypeuri"]:
        if dag_run.conf["scheduletypeuri"].endswith("shift"):
            return [{
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": {
                        "officeScheduleUri": null,
                        "name": null
                    },
                    "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                },
                "effectiveDate": null
            }]
        return [{
                "schedulePolicy": {
                    "officeScheduleUri": dag_run.conf["scheduletypeuri"],
                    "name": null,
                    "officeSchedule": {
                        "officeScheduleUri": dag_run.conf["scheduletypeuri"],
                        "name": null
                    },
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                },
                "effectiveDate": null
            }]
    return null


def get_add_user_payload(dag_run):
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf["loginname"],
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf["firstname"],
            "lastname": dag_run.conf["lastname"],
            "emailAddress": dag_run.conf["emailaddress"],
            "employeeId": dag_run.conf["employeeid"],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": get_schedule_policy(dag_run),
            "workWeekStartDayUri": dag_run.conf["workweek"],
            "employmentDateRange": {
                "startDate": rail.parse_date(dag_run.conf["startdate"], MMDDYYY),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [dag_run.conf["authenticationtype"]],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf["loginname"],
                "SSOName": dag_run.conf["loginname"] if "sso" in dag_run.conf["authenticationtype"] else null,
                "password": null
            },
            "holidayCalendar": {
                "uri": dag_run.conf["holidaycalendar"],
                "name": null,
            } if dag_run.conf["holidaycalendar"] else null,
            "timeOffPolicy": null,
            "permissionSets": [
                {
                    "uri": null,
                    "name": "Project Resource with Reports"
                }
            ],
            "policySets": get_policy_sets(dag_run),
            "employeeType": null,
            "costRateSchedule": {
                "initialHourlyRate": {
                    "amount": dag_run.conf["hourlycostamount"],
                    "currency": {
                        "uri": dag_run.conf["hourlycostcurrency"],
                        "name": null,
                        "symbol": null
                    }
                },
                "scheduleEntries": []
            } if dag_run.conf["hourlycostamount"] and dag_run.conf["hourlycostcurrency"] else null,
            "payrollRateSchedule": {
                "initialHourlyRate": {
                    "amount": dag_run.conf["hourlypayrate"],
                    "currency": {
                        "uri": dag_run.conf["hourlypayratecurrency"],
                        "name": null,
                        "symbol": null
                    }
                },
                "scheduleEntries": []
            } if dag_run.conf["hourlypayrate"] and dag_run.conf["hourlypayratecurrency"] else null,
            "timesheetPeriodTypeUri": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": {
                "uri": dag_run.conf["timesheetapproval"],
                "name": null
            } if dag_run.conf["timesheetapproval"] else null,
            "expenseApprovalPath": null,
            "timeOffApprovalPath": {
                "uri": dag_run.conf["timeoffapproval"],
                "name": null
            } if dag_run.conf["timeoffapproval"] else null,
            "customFieldValues": get_custom_field_values(dag_run),
            "assignedActivities": get_activity_list(dag_run),
            "timeZone": {
                "uri": dag_run.conf["timezone"],
                "IANAName": null
            } if dag_run.conf["timezone"] else null,
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule":  [{
                "location": {
                    "uri": dag_run.conf["locationcode"],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }] if dag_run.conf["locationcode"] else null,
            "divisionSchedule":  [{
                "division": {
                    "uri": dag_run.conf["businessunitcode"],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }]if dag_run.conf["businessunitcode"] else null,
            "costCenterSchedule":  [{
                "costCenter": {
                    "uri": dag_run.conf["financecostcentercode"],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }] if dag_run.conf["financecostcentercode"] else null,
            "serviceCenterSchedule": [{
                "serviceCenter": {
                    "uri": dag_run.conf["legalemployercode"],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }] if dag_run.conf["legalemployercode"] else null,
            "departmentGroupSchedule": [{
                "departmentGroup": {
                    "uri": dag_run.conf["departmentcode"],
                    "parent": null,
                    "name": null
                },
                "effectiveDate": null
            }] if dag_run.conf["departmentcode"] else null,
            "employeeTypeGroupSchedule": [{
                "employeeTypeGroup": {
                    "uri": dag_run.conf["paygroupcode"],
                    "parent": null,
                    "name": null
                },
                "effectiveDate": null
            }] if dag_run.conf["paygroupcode"] else null,
            "timesheetPeriodSchedule": [{
                "timesheetPeriod": {
                    "uri": dag_run.conf["timesheetperiod"],
                    "name": null
                },
                "effectiveDate": null
            }] if dag_run.conf["timesheetperiod"] else null,
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": get_payrule_schedule(dag_run),
            "displayNameParameter": {
                "displayName": dag_run.conf["displayname"]
            }
        }
    }


def get_department_update_payload(dag_run):
    return {
        "user": {
            "uri":  dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "departmentGroupScheduleToApply": {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": {
                                "uri": dag_run.conf["departmentcode"],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": rail.result("get_effective_date")
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_paygroup_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "employeeTypeGroupScheduleToApply": {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {
                                "uri": dag_run.conf["paygroupcode"],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": rail.result("get_effective_date")
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_business_units_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "divisionScheduleToApply": {
                "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDivisionSchedule": [],
                "updateDivisionScheduleOverDateRange": {
                    "replacementDivisionScheduleEntries": [
                        {
                            "division": {
                                "uri": dag_run.conf["businessunitcode"],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate":rail.result("get_effective_date")
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_location_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "locationScheduleToApply": {
                "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementLocationSchedule": [],
                "updateLocationScheduleOverDateRange": {
                    "replacementLocationScheduleEntries": [
                        {
                            "location": {
                                "uri": dag_run.conf["locationcode"],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": rail.result("get_effective_date")
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_finance_costcenter_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementCostCenterSchedule": [],
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {
                                "uri": dag_run.conf["financecostcentercode"],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": rail.result("get_effective_date")
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_legal_employer_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "serviceCenterScheduleToApply": {
                "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementServiceCenterSchedule": [],
                "updateServiceCenterScheduleOverDateRange": {
                    "replacementServiceCenterScheduleEntries": [
                        {
                            "serviceCenter": {
                                "uri": dag_run.conf["legalemployercode"],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate":  rail.result("get_effective_date")
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_activities_payload(dag_run):
    activities_list = list(map(lambda i: {"name": i},
                               custom_methods.get_valid_activity_names(dag_run)))
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "activitiesToApply": activities_list
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_payrule_update_schedule(dag_run):
    payrule = get_payrule_schedule(dag_run)
    payrule[0]["effectiveDate"] = rail.result("get_effective_date")
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "payRulesScheduleModifications": {
                "scheduleEntries":
                        payrule
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_timesheet_period_update(dag_run):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "timesheetPeriodScheduleToApply": {
                "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementTimesheetPeriodSchedule": [],
                "updateTimesheetPeriodScheduleOverDateRange": {
                    "replacementTimesheetPeriodScheduleEntries": [
                        {
                            "timesheetPeriod": {
                                "uri": dag_run.conf["timesheetperiod"],
                                "name": null
                            },
                            "effectiveDate": rail.result("get_effective_date")
                        }
                    ]
                },
                "projectRolesToApply": null
            },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }
    }


def get_shift_schedule_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
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
                                "officeSchedule": null,
                                "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                            },
                            "effectiveDate": rail.result("get_effective_date")
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_update_office_schedule_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "schedulePolicyToApply": {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementSchedule": [],
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": dag_run.conf["scheduletypeuri"],
                                "name": null,
                                "officeSchedule": {
                                    "officeScheduleUri": dag_run.conf["scheduletypeuri"],
                                    "name": null
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate":rail.result("get_effective_date")
                        }
                    ],
                    "endDate": null
                }
            },
            "projectRolesToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
