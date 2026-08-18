from datetime import datetime
import rail
from pwcglobal.user_import_australia import custom_methods


def get_manager_details_payload():
    return {
        "users": [
            {
                "uri": rail.result('get_user_details')[0]['userDetails']['supervisor']['uri'],
                "loginName": None,
                "parameterCorrelationId": None
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
    }


def get_get_data_payload(dag_run):
    return{
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": None,
                "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": {
                    "uri": None,
                    "uris": [],
                    "bool": None,
                    "date": None,
                    "money": None,
                    "number": None,
                    "text": dag_run.conf["guid"],
                    "time": None,
                    "calendarDayDurationValue": None,
                    "workdayDurationValue": None,
                    "dateRange": None,
                    "dateTimeUtc": None
                },
                "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        }
    }


def get_update_employment_date_range_payload(dag_run):
    start_date = rail.result("get_user_details")[
        'employmentDateRange']['startDate']
    end_date = custom_methods.convert_to_date(
        dag_run.conf['termination_date'], "%d-%m-%Y")
    return{
        "userUri": rail.result("get_users_data")[0]["user_uri"],
        "dateRange": {
            "startDate": {
                "year": start_date['year'],
                "month": start_date['month'],
                "day": start_date['day']
            },
            "endDate": {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day
            },
            "relativeDateRangeUri": None,
            "relativeDateRangeAsOfDate": None
        }
    }


def get_put_cost_center_schedule_for_user_with_both_dates_payload(dag_run):
    compensation_date_to_apply = custom_methods.convert_to_date(
        dag_run.conf['compensation_plan_effective_date'], "%d-%m-%Y")
    end_date_to_apply = custom_methods.convert_to_date(rail.result(
        'process_expected_end_date')['expected_end_date'], "%d-%m-%Y")
    return{
        "userUri": rail.result('get_users_data')[0]['user_uri'],
        "scheduleEntries": [
            {
                "costCenter": {
                    "uri": rail.result('get_enabled_cost_centers')['yes_uri'],
                    "parentUri": None,
                    "name": None
                },
                "effectiveDate": {
                    "year": compensation_date_to_apply.year,
                    "month": compensation_date_to_apply.month,
                    "day": compensation_date_to_apply.day
                }
            },
            {
                "costCenter": {
                    "uri": rail.result('get_enabled_cost_centers')['no_uri'],
                    "parentUri": None,
                    "name": None
                },
                "effectiveDate": {
                    "year": end_date_to_apply.year,
                    "month": end_date_to_apply.month,
                    "day": end_date_to_apply.day
                }
            }
        ]
    }


def get_add_cost_center_schedule_for_user_applyModification(dag_run):
    replacement_schedule = []
    custom_methods.add_yes(dag_run, replacement_schedule, "costCenter")
    custom_methods.add_no(replacement_schedule, "costCenter")
    payload = {
        "user": {
            "uri": rail.result('get_users_data')[0]['user_uri'],
        },
        "modifications": {
            "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementCostCenterSchedule": [],
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": replacement_schedule,
                    "endDate": None
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

    return payload


def get_add_business_units_schedule_for_user_applyModification(dag_run):
    replacement_schedule = []
    custom_methods.add_yes(dag_run, replacement_schedule, "division")
    custom_methods.add_no(replacement_schedule, "division")
    payload = {
        "user": {
            "uri": rail.result('get_users_data')[0]['user_uri'],
        },
        "modifications": {
            "divisionScheduleToApply": {
                "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDivisionSchedule": [],
                "updateDivisionScheduleOverDateRange": {
                    "replacementDivisionScheduleEntries":  replacement_schedule,
                    "endDate": None
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

    return payload


def get_add_classifications_schedule_for_user_applyModification(dag_run):
    replacement_schedule = []
    if dag_run.conf['compensation_plan_effective_date']:
        replacement_schedule.append({
            "serviceCenter": {
                "uri": rail.result("get_enabled_classifications")['uri'],
                "parentUri": None,
                "name": None
            },
            "effectiveDate": custom_methods.get_payload_format_date(custom_methods.convert_to_date(
                dag_run.conf['compensation_plan_effective_date'], "%d-%m-%Y"))
        })

    if dag_run.conf['expected_end_date'] and rail.result('get_previous_service_center_schedule_for_user'):
        replacement_schedule.append({
            "serviceCenter": {
                "uri": rail.result('get_previous_service_center_schedule_for_user')['uri'],
                "parentUri": None,
                "name": None
            },
            "effectiveDate": custom_methods.get_payload_format_date(custom_methods.convert_to_date(dag_run.conf['expected_end_date'], "%d-%m-%Y"))
        })

    return {
        "user": {
            "uri": rail.result('get_users_data')[0]['user_uri'],
        },
        "modifications": {
            "serviceCenterScheduleToApply": {
                "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementServiceCenterSchedule": [],
                "updateServiceCenterScheduleOverDateRange": {
                    "replacementServiceCenterScheduleEntries":  replacement_schedule,
                    "endDate": None
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_replicon_date(date_str):
    date = datetime.strptime(date_str, '%d-%m-%Y')
    return {
        'year': date.year,
        'month': date.month,
        'day': date.day
    }


def get_put_user_payload(dag_run):
    return {
        "user": {
            "target": {
                "uri": None,
                "loginName": dag_run.conf['guid'],
                "parameterCorrelationId": None
            },
            "firstname": dag_run.conf['firstname'],
            "lastname": dag_run.conf['lastname'],
            "emailAddress": dag_run.conf['work_email'],
            "employeeId": dag_run.conf['employee_id'],
            "department": None,
            "supervisorAssignmentSchedule": None,
            "schedulePolicySchedule": [],
            "workWeekStartDayUri": None,
            "employmentDateRange": {
                "startDate": get_replicon_date(dag_run.conf['hire_date']),
                "endDate": None,
                "relativeDateRangeUri": None,
                "relativeDateRangeAsOfDate": None
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['guid'],
                "SSOName": dag_run.conf['guid'],
                "password": None
            },
            "holidayCalendar": None,
            "timeOffPolicy": None,
            "permissionSets": [
                {
                    "uri": None,
                    "name": "AUS - User"
                }
            ],
            "policySets": [],
            "employeeType": None,
            "timesheetPeriodTypeUri": None,
            "costRateSchedule": None,
            "payrollRateSchedule": None,
            "defaultBillingRate": None,
            "timesheetApprovalPath": None,
            "expenseApprovalPath": None,
            "timeOffApprovalPath": {
                "uri": None,
                "name": "Supervisor"
            },
            "customFieldValues": [],
            "assignedActivities": [],
            "timeZone": None,
            "overtimeRuleAssignmentSchedule": None,
            "validationRuleAssignmentSchedule": None,
            "locationSchedule": [],
            "divisionSchedule": [],
            "costCenterSchedule": [],
            "serviceCenterSchedule": [],
            "departmentGroupSchedule": [],
            "employeeTypeGroupSchedule": [],
            "timesheetPeriodSchedule": [{
                "timesheetPeriod": {
                    "uri": None,
                    "name": "Weekly starting on Monday"
                },
                "effectiveDate": None
            }] if dag_run.conf['active_status'] and (dag_run.conf['active_status']).lower() == "yes" else [],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": []
        }
    }


def get_activity_assignment_payload(dag_run, caller, user_uri):
    return {
        "userUri": user_uri if user_uri else rail.result("add_new_user")["uri"],
        "activityUris": custom_methods.get_activity_uris(dag_run, caller, parent="add_user")
    }


def get_update_text_value_payload(dag_run, custom_field, user_uri=None):
    return{
        "objectUri": dag_run.conf['user_uri'] if user_uri else rail.result('add_new_user')['uri'],
        "customFieldUri": dag_run.conf[custom_field + "_customfield_uri"],
        "value": dag_run.conf[custom_field]
    }


def get_search_service_center_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:service-center-list-column:service-center",
            "urn:replicon:location-list-column:effectively-enabled",
            "urn:replicon:location-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": None,
                "filterDefinitionUri": "urn:replicon:service-center-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": {
                    "uri": None,
                    "uris": [],
                    "bool": None,
                    "date": None,
                    "money": None,
                    "number": None,
                    "text": dag_run.conf['classification'],
                    "time": None,
                    "calendarDayDurationValue": None,
                    "workdayDurationValue": None,
                    "dateRange": None,
                    "dateTimeUtc": None
                },
                "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        }
    }


def get_search_department_group_by_name_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:location-list-column:code",
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:effectively-enabled",
            "urn:replicon:department-group-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": None,
                    "operatorUri": None,
                    "rightExpression": None,
                    "value": None,
                    "filterDefinitionUri": "urn:replicon:department-group-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "leftExpression": None,
                    "operatorUri": None,
                    "rightExpression": None,
                    "value": {
                        "uri": None,
                        "uris": [],
                        "bool": None,
                        "date": None,
                        "money": None,
                        "number": None,
                        "text": dag_run.conf['costcenter_name'],
                        "time": None,
                        "calendarDayDurationValue": None,
                        "workdayDurationValue": None,
                        "dateRange": None,
                        "dateTimeUtc": None
                    },
                    "filterDefinitionUri": None
                },
                "value": None,
                "filterDefinitionUri": None
            },
            "operatorUri": "urn:replicon:filter-operator:or",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": None,
                    "operatorUri": None,
                    "rightExpression": None,
                    "value": None,
                    "filterDefinitionUri": "urn:replicon:department-group-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "leftExpression": None,
                    "operatorUri": None,
                    "rightExpression": None,
                    "value": {
                        "uri": None,
                        "uris": [],
                        "bool": None,
                        "date": None,
                        "money": None,
                        "number": None,
                        "text": dag_run.conf['costcenter_id'],
                        "time": None,
                        "calendarDayDurationValue": None,
                        "workdayDurationValue": None,
                        "dateRange": None,
                        "dateTimeUtc": None
                    },
                    "filterDefinitionUri": None
                },
                "value": None,
                "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        }
    }


def get_search_location_group_by_name_payload(dag_run, location):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:effectively-enabled",
            "urn:replicon:location-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": None,
                "filterDefinitionUri": "urn:replicon:location-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": {
                    "uri": None,
                    "uris": [],
                    "bool": None,
                    "date": None,
                    "money": None,
                    "number": None,
                    "text": dag_run.conf[location],
                    "time": None,
                    "calendarDayDurationValue": None,
                    "workdayDurationValue": None,
                    "dateRange": None,
                    "dateTimeUtc": None
                },
                "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        }
    }


def get_data_access_scope_payload():
    return{
        "userUri": rail.result('add_new_user')['uri'],
        "policyDataAccessScopes": [
            {
                "policyUri": "urn:replicon:policy:time-off",
                "locations": [
                    {
                        "location": {
                            "uri": rail.result('search_location_group2_by_name_code')[0]['uri'],
                            "parentUri": None,
                            "name": None
                        },
                        "groupSpecificationModeUri": None,
                        "groupDescendantModeUri": None
                    }
                ],
                "divisions": [],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [],
                "employeeTypeGroups": []
            }
        ]
    }


def get_search_supervisor_payload(dag_run):
    return{
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": None,
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": {
                    "uri": None,
                    "uris": [],
                    "bool": None,
                    "date": None,
                    "money": None,
                    "number": None,
                    "text": dag_run.conf['manager_id'],
                    "time": None,
                    "calendarDayDurationValue": None,
                    "workdayDurationValue": None,
                    "dateRange": None,
                    "dateTimeUtc": None,
                    "dateTimeUtcRange": None,
                    "numberRange": None
                },
                "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        }
    }

# pylint: disable=too-many-statements


def get_user_modification_custom_payload_with_logs(dag_run):
    log_list = []
    error_list = []

    def update_holiday_calender():
        assigned_holiday_calender = rail.result('get_user_details')[
            0]['holidayCalendar']
        if rail.result('get_holiday_calender_uri') and \
                dag_run.conf['location_level_2'] != (assigned_holiday_calender['displayText'] if assigned_holiday_calender else ""):

            log_list.append("Holiday calendar updated")
            return{
                "holidayCalendar": {
                    "uri": rail.result('get_holiday_calender_uri'),
                    "name": None
                }
            }
        return None

    def get_timesheet_period_payload():
        timesheet_schedule = rail.result('get_timesheet_schedule_for_user')
        user_timesheet_period = timesheet_schedule[-1]['timesheetPeriod']['displayText'] if (timesheet_schedule
                                                                                             and timesheet_schedule[-1]['timesheetPeriod']) else None
        timesheet_period_to_apply = "Weekly starting on Monday"
        if dag_run.conf['active_status'] == "Yes" and timesheet_period_to_apply != (user_timesheet_period if user_timesheet_period else ""):
            log_list.append("Timesheet period updated")
            return {
                "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementTimesheetPeriodSchedule": [],
                "updateTimesheetPeriodScheduleOverDateRange": {
                    "replacementTimesheetPeriodScheduleEntries": [
                        {
                            "timesheetPeriod": {
                                "uri": None,
                                "name": timesheet_period_to_apply
                            },
                            "effectiveDate": custom_methods.get_payload_format_date(datetime.now())
                        }
                    ],
                    "endDate": None
                }
            }

        return None

    employee_type = dag_run.conf['employee_type'] + \
        ' - ' + dag_run.conf['time_type']

    def get_employee_type_schedule_payload():
        employee_type_uri = rail.find_first_by_attr_and_get_attr(rail.result(
            "get_all_employee_type_details"), "displayText", employee_type, 'uri')
        assigned_employee_type = rail.result('get_effectivegroup_membership')[
            'employeeTypes'][0]['employeeType'] if rail.result('get_effectivegroup_membership')['employeeTypes'] else None
        if not employee_type_uri:
            log_list.append(
                f"Employee type was not updated as the employee type {employee_type} not found in Replicon")
            error_list.append(
                f"Employee type was not updated as the employee type {employee_type} not found in Replicon")
            return None

        if employee_type_uri and employee_type != (assigned_employee_type['employeeType']['displayText'] if assigned_employee_type else ""):
            log_list.append("Employee type updated")
            return {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {
                                "uri": employee_type_uri,
                            },
                            "effectiveDate": custom_methods.get_payload_format_date(datetime.now())
                        }
                    ],
                    "endDate": None
                }
            }
        return None

    current_location = rail.result('get_effectivegroup_membership')['locations'][0]['location'] if rail.result(
        'get_effectivegroup_membership')['locations'] != [] else None
    location_toassgin = dag_run.conf['location_level_1']
    location_changed = (current_location['location']['displayText']
                        if current_location else "") != location_toassgin

    def update_timezone_payload():
        location_timezone = custom_methods.get_mapper_timezone_for_location(
            dag_run)
        if location_timezone:
            timezone_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_timezones'), 'displayText', location_timezone['timezone'])
            if timezone_uri and location_changed:
                log_list.append("Time zone updated")
                return {
                    "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                    "timezone": {
                        "uri": timezone_uri['uri'],
                        "IANAName": None
                    }
                }
        return None

    def get_location_schedule_payload():
        location_toassgin_uri = rail.result('get_location_uri')
        if not location_toassgin_uri:
            log_list.append(
                f"Location was not updated as the location group {location_toassgin} not found in Replicon")
            error_list.append(
                f"Location was not updated as the location group {location_toassgin} not found in Replicon")
            return None

        if location_toassgin_uri and location_changed:
            log_list.append("Location group (countries)updated")
            return {
                "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementLocationSchedule": [],
                "updateLocationScheduleOverDateRange": {
                    "replacementLocationScheduleEntries": [
                        {
                            "location": {
                                "uri": location_toassgin_uri[0]['uri'],
                                "parentUri": None,
                                "name": None
                            },
                            "effectiveDate": custom_methods.get_payload_format_date(datetime.now())
                        }
                    ],
                    "endDate": None
                }
            }
        return None

    assigned_classification = rail.result('get_effectivegroup_membership')[
        'serviceCenters'][0]['serviceCenter'] if rail.result('get_effectivegroup_membership')['serviceCenters'] else None
    classification_uri_toassign = rail.result("get_classification_uri")
    classification_changed = (assigned_classification['serviceCenter']['displayText']
                              if assigned_classification else "") != dag_run.conf['classification']

    def get_service_center_payload():
        if classification_uri_toassign and classification_changed:
            log_list.append("Classification (service center)updated")
            return {
                "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementServiceCenterSchedule": [],
                "updateServiceCenterScheduleOverDateRange": {
                    "replacementServiceCenterScheduleEntries": [
                        {
                            "serviceCenter": {
                                "uri": classification_uri_toassign,
                                "parentUri": None,
                                "name": None
                            },
                            "effectiveDate": custom_methods.get_payload_format_date(datetime.now())
                        }
                    ],
                    "endDate": None
                }
            }
        return None

    def get_payroll_rate_modification_payload():
        data = custom_methods.get_mapper_classification_records(dag_run)
        if data and (classification_uri_toassign and classification_changed) and data['hourly rate']:
            log_list.append("Hourly cost rate updated")
            return {
                "scheduleEntriesToAdd": [
                    {
                        "hourlyRate": {
                            "amount": data['hourly rate'],
                            "currency": {
                                "uri": rail.result('get_aus_currency'),
                                "name": None,
                                "symbol": None
                            }
                        },
                        "effectiveDate": custom_methods.get_payload_format_date(datetime.now())
                    }
                ]}
        return None

    def get_office_schedule_payload():
        last_assigned_office_schedule = rail.result(
            'get_user_details')[0]['schedulePolicies']

        if not rail.result('get_office_schedule_uri'):
            log_list.append(
                f"Schedule was not updated as the office schedule id {dag_run.conf['id']} not found in Replicon")
            return None
        if dag_run.conf['id'] and ((last_assigned_office_schedule[-1
                                    ]['officeSchedule']['displayText'] if last_assigned_office_schedule else "") != dag_run.conf['id']):
            log_list.append("Office schedule is updated")
            return {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementSchedule": [],
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": None,
                                "name": None,
                                "officeSchedule": {
                                    "officeScheduleUri": rail.result('get_office_schedule_uri'),
                                    "name": None
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": custom_methods.get_payload_format_date(datetime.now())
                        }
                    ],
                    "endDate": None
                }
            }
        return None

    costcenter_name = dag_run.conf['costcenter_name']
    assigned_costcenter_name = rail.result('get_effectivegroup_membership')['departments'][0]['department']['department']\
        if rail.result('get_effectivegroup_membership')['departments'] else {'displayText': ""}
    department_uri = rail.result('get_department_uri')[
        0]['uri'] if rail.result('get_department_uri') else None

    def get_scripts_to_assign():
        if costcenter_name and (costcenter_name != assigned_costcenter_name['displayText']) and department_uri:
            return custom_methods.get_activity_uris(dag_run, method_caller='non_salaried_self_employed'
                if employee_type == "Non-Salaried / Self-Employed - Full time" else 'regular_fixed_term', parent="update_user")
        return None

    def get_update_payrule_script_payload():
        initial_payrule = custom_methods.get_entries_from_user_mapper(dag_run)
        latest_assigned_payrule = rail.result("get_user_details")[
            0]['payRuleScriptSchedule']
        if costcenter_name and (costcenter_name != assigned_costcenter_name['displayText']) and department_uri:
            # pylint: disable=line-too-long
            if initial_payrule and (((latest_assigned_payrule[-1]['payRuleScript']['displayText'] if latest_assigned_payrule else "") != initial_payrule['Initialpayrulename']) if latest_assigned_payrule else True):

                log_list.append("Pay rule updated")
                return {
                    "initialPayRule": None,
                    "scheduleEntries": [
                        {
                            "payRuleScript": {
                                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts'),
                                                                            "displayText", initial_payrule['Initialpayrulename'], 'uri'),
                                "name": None
                            },
                            "effectiveDate": custom_methods.get_payload_format_date(datetime.now())
                        }
                    ]
                }
        return None

    def get_department_group_payload():
        if not department_uri:
            log_list.append(
                f"Country code was not updated as the department group(cost center) {costcenter_name} not found in Replicon")
            error_list.append(
                f"Country code was not updated as the department group(cost center) {costcenter_name} not found in Replicon")

        if costcenter_name and (costcenter_name != assigned_costcenter_name['displayText']) and department_uri:
            log_list.append("Department group (cost center)updated")
            return{
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": {
                                "uri": department_uri,
                                "parent": None,
                                "name": None,
                                "parameterCorrelationId": None
                            },
                            "effectiveDate": custom_methods.get_payload_format_date(datetime.now())
                        }
                    ],
                    "endDate": None
                }
            }
        return None

    def get_update_user_details_payload():
        is_first_name_changed = dag_run.conf['firstname'] != rail.result(
            'get_user_details')[0]['userDetails']['firstName']
        is_last_name_changed = dag_run.conf['lastname'] != rail.result(
            'get_user_details')[0]['userDetails']['lastName']
        is_email_changed = dag_run.conf['work_email'] != rail.result(
            'get_user_details')[0]['userDetails']['emailAddress']

        def get_first_name():
            if is_first_name_changed:
                log_list.append("First name updated")
                return dag_run.conf['firstname']
            return None

        def get_last_name():
            if is_last_name_changed:
                log_list.append("Last name updated")
                return dag_run.conf['lastname']
            return None

        def get_email():
            if is_email_changed:
                log_list.append("Email address updated")
                return{
                    "emailAddress": dag_run.conf['work_email']
                }
            return None

        return {
            "firstName": get_first_name(),
            "lastName": get_last_name(),
            "emailAddress": get_email(),
        } if is_first_name_changed or is_last_name_changed or is_email_changed else None

    payload = {
        "user": {
            "uri": dag_run.conf['user_uri']
        },
        "modifications": {
            "timezoneToApply": update_timezone_payload(),
            "holidayCalendarToApply": update_holiday_calender(),
            "schedulePolicyToApply": get_office_schedule_payload(),
            "locationScheduleToApply": get_location_schedule_payload(),
            "departmentGroupScheduleToApply": get_department_group_payload(),
            "employeeTypeGroupScheduleToApply": get_employee_type_schedule_payload(),
            "timesheetPeriodScheduleToApply": get_timesheet_period_payload(),
            "serviceCenterScheduleToApply": get_service_center_payload(),
            "activitiesToApply": get_scripts_to_assign(),
            "payRulesToApply": get_update_payrule_script_payload(),
            "payrollRatesModifications": get_payroll_rate_modification_payload(),
            "customFieldValuesToApply": custom_methods.get_update_custom_field_payload(dag_run, log_list),
            "userDetailsToApply": get_update_user_details_payload()
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
    custom_methods.get_update_user_custom_log_message(dag_run, log_list)
   # main return
    return {
        'payload': payload,
        'logs': log_list,
        'error_logs': error_list,
        "error_logs_length": len(error_list),
        "severity": "Exception" if len(error_list) > 0 else "Success",
        "log_message": "User updated successfully" + (" " if log_list else '') + ";".join(log_list)
    }
