from datetime import datetime
from conduent.user_import.utils import custom_methods
import rail

null = None
true = True


def get_supervisor_details(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-id"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": null,
                        "uris": [],
                        "bool": "true",
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
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": null,
                        "uris": [],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": dag_run.conf["manager_win"],
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
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_create_user_payload(dag_run, config):
    start_date = get_date_from_replicon_date(
        rail.parse_date(dag_run.conf["date_active"], "%m/%d/%Y"))
    end_date = get_date_from_replicon_date(
        rail.parse_date(dag_run.conf["date_termed"], "%m/%d/%Y"))
    custom_fields = []
    custom_fields.append(custom_methods.get_udf_req(
        dag_run.conf["assignment_status_uri"], text=dag_run.conf["assignment_status"]))
    if dag_run.conf["job_title"]:
        custom_fields.append(custom_methods.get_udf_req(
            dag_run.conf["job_title_uri"], text=dag_run.conf["job_title"]
        ))

    return {
        "user": {
            "target": {
                "loginName": dag_run.conf["email"]
            },
            "firstname": dag_run.conf["first_name"],
            "lastname": dag_run.conf["last_name"],
            "emailAddress": dag_run.conf["email"],
            "employeeId": dag_run.conf["win_id"],
            "department": null,
            "supervisorAssignmentSchedule": {
                "initialSupervisor": null,
                "supervisorScheduleEntries": [
                    {
                        "supervisor": {
                            "uri": rail.result("get_supervisor_details"),
                            "loginName": null,
                            "employeeId": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": custom_methods.get_effective_date(config, dag_run)
                    }
                ]
            } if rail.result("get_supervisor_details") else null,
            "schedulePolicySchedule": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": dag_run.conf["work_schedule_name_uri"],
                        "name": null,
                        "officeSchedule": null,
                        "scheduleTypeUri": null
                    },
                    "effectiveDate": custom_methods.get_effective_date(config, dag_run)
                }
            ] if dag_run.conf["work_schedule_name_uri"] else null,
            "workWeekStartDayUri": dag_run.conf["work_week_uri"],
            "employmentDateRange": {
                "startDate": rail.parse_date(dag_run.conf["date_active"], "%m/%d/%Y"),
                "endDate": rail.parse_date(dag_run.conf["date_termed"], "%m/%d/%Y") if start_date < end_date else null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": ['urn:replicon:user-authentication-type:sso'],
                "isLoginEnabled": False,
                "loginName": dag_run.conf["email"],
                "SSOName": dag_run.conf["email"],
                "password": null
            },
            "holidayCalendar": {
                "uri": dag_run.conf["holiday_calendar_uri"],
                "name": null
            } if dag_run.conf["holiday_calendar_uri"] else null,
            "timesheetApprovalPath": {
                "uri": dag_run.conf["timesheet_approval_path"],
                "name": null
            },
            "expenseApprovalPath": null,
            "expenseDefaultReimbursementCurrency": null,
            "timeOffApprovalPath": {
                "uri": dag_run.conf["timeoff_approval_path"],
                "name": null
            },
            "workAuthorizationApprovalPath": null,
            "timeOffBalancePayoutApprovalPath": null,
            "customFieldValues": custom_fields,
            "assignedActivities": [],
            "timeZone": {
                "uri": dag_run.conf["time_zone_uri"],
                "IANAName": null
            },
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [
                {
                    "location": {
                        "uri": dag_run.conf["location_uri"],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": custom_methods.get_effective_date(config, dag_run)
                }
            ]if dag_run.conf["location_uri"] else [],
            "divisionSchedule": [
                {
                    "division": {
                        "uri": dag_run.conf["business_group_uri"],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": custom_methods.get_effective_date(config, dag_run)
                }
            ] if dag_run.conf["business_group_uri"] else null,
            "costCenterSchedule": [
                {
                    "costCenter": {
                        "uri": dag_run.conf["cost_center_schedule_uri"],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": custom_methods.get_effective_date(config, dag_run)
                }
            ] if dag_run.conf["cost_center_schedule_uri"] else null,
            "serviceCenterSchedule": [],
            "departmentGroupSchedule": [],
            "employeeTypeGroupSchedule": [],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": dag_run.conf["timesheet_period"]
                    },
                    "effectiveDate": null
                }
            ],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [],
            "displayNameParameter": {
                "displayName": dag_run.conf["preferred_name"]
            },
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": [],
            "workCompliancePolicyAssignmentSchedule": []
        }
    }


def get_user_payload(config, dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:end-date",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:login-name",
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "bool": "true"
                    },
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:end-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": null,
                            "endDate": custom_methods.get_effective_date(config, dag_run),
                            "relativeDateRangeUri": null,
                            "relativeDateRangeAsOfDate": null
                        }
                    },
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def put_notifications_payload():
    return {
        "user": {
            "uri": rail.result("create_user_in_replicon")["uri"]
        },
        "preferences": {
            "notificationDeliveryPreferences": [
                {
                    "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:project",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:time-off",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:user",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:timesheet",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:time-entry-revision-group",
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


def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])


def get_office_schedule_request(dag_run, config):
    return {
        "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementSchedule": [],
        "updateScheduleOverDateRange": {
            "replacementScheduleEntries": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": dag_run.conf["work_schedule_name_uri"],
                        "name": null,
                        "officeSchedule": {
                            "officeScheduleUri": dag_run.conf["work_schedule_name_uri"],
                            "name": null
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": custom_methods.get_effective_date(config, dag_run)
                }
            ],
            "endDate": null
        }
    }


def get_group_schedule_data(dag_run, config, group_name):

    if group_name == "costCenter":
        update_key = "userCostCenterScheduleModificationOptionUri"
        date_range_key = "updateCostCenterScheduleOverDateRange"
        replacement_key = "replacementCostCenterScheduleEntries"
        group_uri = "cost_center_schedule_uri"
    if group_name == "division":
        group_uri = "business_group_uri"
        update_key = "userDivisionScheduleModificationOptionUri"
        date_range_key = "updateDivisionScheduleOverDateRange"
        replacement_key = "replacementDivisionScheduleEntries"
    if group_name == "location":
        update_key = "userLocationScheduleModificationOptionUri"
        date_range_key = "updateLocationScheduleOverDateRange"
        replacement_key = "replacementLocationScheduleEntries"
        group_uri = "location_uri"
    return {
        update_key: "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        date_range_key: {
            replacement_key: [
                {
                    group_name: {
                        "uri": dag_run.conf[group_uri],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": custom_methods.get_effective_date(config, dag_run)
                }
            ],
            "endDate": null
        }
    }


def get_group_udpdate(dag_run, group_name):
    if group_name == "location" and dag_run.conf["location_uri"] and (
            not rail.result("get_current_group_membership")[
                "existinglocationuri"]
            or rail.result("get_current_group_membership")["existinglocationuri"] != dag_run.conf["location_uri"]):
        return True
    if group_name == "costcenter" and dag_run.conf["cost_center_schedule_uri"] and (
            not rail.result("get_current_group_membership")[
                "existingcostcentersuri"]
            or rail.result("get_current_group_membership")["existingcostcentersuri"] != dag_run.conf["cost_center_schedule_uri"]):
        return True
    if group_name == "division" and dag_run.conf["business_group_uri"] and (
            not rail.result("get_current_group_membership")[
                "existingbusinessgroupuri"]
            or rail.result("get_current_group_membership")["existingbusinessgroupuri"] != dag_run.conf["business_group_uri"]):
        return True
    return False

def get_update_user_payload(dag_run, config, action="update_user"):
    schedule_update = custom_methods.check_if_schedule_update(
        dag_run, config)
    location_update = get_group_udpdate(dag_run, "location")
    costcenter_update = get_group_udpdate(dag_run, "costcenter")
    division_update = get_group_udpdate(dag_run, "division")
    custom_field_update, custom_field_log = custom_methods.get_custom_fields_update(
        dag_run)
    basic_attribute_update, basic_attribute_log = custom_methods.check_if_user_attribute_update(
        config,dag_run)
    msg = ""
    if schedule_update:
        msg = "Office schedule updated;"
    if location_update:
        msg += "Location updated;"
        msg += "Holiday Calendar update;"
    if costcenter_update:
        msg += "Cost center updated;"
    if division_update:
        msg += "Business Group updated;"
    if custom_field_update:
        msg += custom_field_log
    if basic_attribute_update:
        msg += basic_attribute_log
    if action == "logs":
        return msg
    return {
        "user": {
            "uri": rail.result("get_user_details")["userDetails"]["uri"],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "timezoneToApply": null,
            "workWeekStartToApply": null,
            "holidayCalendarToApply": {
                "holidayCalendar": {
                    "uri": dag_run.conf["holiday_calendar_uri"],
                    "name": null
                }
            } if location_update and dag_run.conf["holiday_calendar_uri"] else null,
            "holidayCalendarAssignmentsToApply": null,
            "schedulePolicyToApply": get_office_schedule_request(dag_run, config) if schedule_update else null,
            "locationScheduleToApply": get_group_schedule_data(dag_run, config, "location") if location_update else null,
            "divisionScheduleToApply": get_group_schedule_data(dag_run, config, "division") if division_update else null,
            "costCenterScheduleToApply": get_group_schedule_data(dag_run, config, "costCenter") if costcenter_update else null,
            "customFieldValuesToApply": custom_field_update if custom_field_update else null,
            "userDetailsToApply": basic_attribute_update if basic_attribute_update else null,
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
