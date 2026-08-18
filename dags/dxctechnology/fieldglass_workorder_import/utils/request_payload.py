from dxctechnology.fieldglass_workorder_import.utils import custom_methods
import rail
null = None


def get_division_update_request(dag_run):
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
                                "uri": dag_run.conf["costcenteruri"],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": custom_methods.replicon_effective_date()
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_division_update_request_compass(dag_run):
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
                                "uri": dag_run.conf["costcenteruri"],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": custom_methods.replicon_effective_date_compass()
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_timesheet_period_value(dag_run):
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
                                "uri": null,
                                "name": "Weekly - Starting Saturday - CSC Contractors, US and Canada employees"
                            },
                            "effectiveDate": custom_methods.replicon_effective_date()
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_timesheet_period_value_compass(dag_run):
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
                                "uri": null,
                                "name": "Weekly - Starting Monday - ES employees and Contractors"
                            },
                            "effectiveDate": custom_methods.replicon_effective_date_compass()
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_activities_request(dag_run):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "activitiesToApply2": {
                "activitiesToAssign": [
                    {
                        "uri": null,
                        "name": "799-Contractor not available"
                    }
                ],
                "activitiesToRemove": []
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_activities_request_compass(dag_run, config):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "activitiesToApply2": {
                "activitiesToAssign": custom_methods.get_compass_activity_list(config),
                "activitiesToRemove": []
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_compass_workweek_request(dag_run):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "workWeekStartToApply": {
                "workWeekStartDayUri": {"C1": "urn:replicon:day-of-week:saturday",
                                        "ES" : "urn:replicon:day-of-week:monday"}[dag_run.conf["FinanceSystem"]]
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_division_update_request_compass_po_bolb(dag_run):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "employeeTypeGroupScheduleToApply": {
                "userEmployeeTypeGroupScheduleModificationOptionUri": \
                    "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {
                                "uri": dag_run.conf["EmployeetypegroupURI"],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": custom_methods.replicon_effective_date_compass()
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_gsap_workweek_request(dag_run):
    workweek = ""
    if dag_run.conf["FinanceSystem"] == "GSAP":
        workweek = "urn:replicon:day-of-week:saturday"
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "workWeekStartToApply": {
                "workWeekStartDayUri": workweek
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_division_update_request_gsap(dag_run):
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
                                "uri": dag_run.conf["companycodeuri"],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": custom_methods.replicon_effective_date()
                        }
                    ],
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_costcenter_update_request_gsap(dag_run):
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
                                "uri": dag_run.conf["costcenteruri"],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": custom_methods.replicon_effective_date()
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_timesheet_period_value_gsap(dag_run):
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
                                "uri": null,
                                "name": "Weekly - Starting Saturday - CSC Contractors, US and Canada employees"
                            },
                            "effectiveDate": custom_methods.replicon_effective_date()
                        }
                    ],
                    "endDate": null
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_activities_request_gsap(dag_run, config):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "activitiesToApply2": {
                "activitiesToAssign": custom_methods.get_gsap_activity_list(config),
                "activitiesToRemove": []
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_costcenter_assignment_gsap(dag_run):
    return {
        "userUri": dag_run.conf["useruri"],
        "scheduleEntries": [
            {
                "costCenter": {
                    "uri": dag_run.conf["costcenteruri"],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }
        ]
    }

def get_division_assignment_gsap(dag_run):
    return {
        "userUri": dag_run.conf["useruri"],
        "scheduleEntries": [
            {
                "division": {
                    "uri": dag_run.conf["companycodeuri"],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }
        ]
    }

def get_workorderid_update_gsap(dag_run):
    return  {
        "user": {
            "uri": dag_run.conf["useruri"]
        },
        "modifications": {
                "customFieldValuesToApply": [
                    {
                        "customField": {
                            "uri": rail.result("get_bulk_user_details")["workorderiduri"],
                            "name": null,
                            "groupUri": null
                        },
                        "text": dag_run.conf["WorkOrderID"],
                        "date": null,
                        "dropDownOption": null,
                        "number": null
                    }
                ],
                },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_perner_update_gsap(dag_run):
    return  {
        "user": {
            "uri": dag_run.conf["useruri"]
        },
        "modifications": {
                "customFieldValuesToApply": [
                    {
                        "customField": {
                            "uri": rail.result("get_bulk_user_details")["pernruri"],
                            "name": null,
                            "groupUri": null
                        },
                        "text": dag_run.conf["GHR_personnel_number"],
                        "date": null,
                        "dropDownOption": null,
                        "number": null
                    }
                ],
                },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_timesheet_approval_gsap(dag_run):
    return {
        "user": {
            "uri": dag_run.conf["useruri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
                "timesheetApprovalPathToApply": {
                    "uri": null,
                    "name": "GSAP Aus Contractor"
                }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
