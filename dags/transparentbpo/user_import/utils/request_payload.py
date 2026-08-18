import rail
from uuid import uuid4

null = None


def get_process_each_user_payload(item):
    timestamps = rail.result("log_job_start_timestamps")

    conf = {
        'id': item.get('id'),
        "supervisor_permission_set_uri": rail.result('get_supervisor_permission_set_uri'),
        'custom_field_uris': rail.result('get_all_custom_fields'),
        'job_run_date': timestamps['run_date'],
        'log_timestamp': timestamps['log_timestamp']
    }
    return conf


def get_process_update_or_add_user_payload(user_bamboohr_data, user_bamboohr_table_data, action, dag_run, config):
    conf = {
        **user_bamboohr_data,
        "supervisor_permission_set_uri": dag_run.conf['supervisor_permission_set_uri'],
        "custom_field_uris": dag_run.conf['custom_field_uris'],
        "customDirectIndirect": user_bamboohr_table_data.get('customDirectIndirect', ''),
        "customProjectName": user_bamboohr_table_data.get('customProjectName', ''),
        "customClientName": user_bamboohr_table_data.get('customClientName', ''),
        "overtime": user_bamboohr_data.get('flsaEmployeeExemption'),
        "user_name": user_bamboohr_data.get('firstName', '') + ' ' + user_bamboohr_data.get('middleName', '') + ' ' + user_bamboohr_data.get('lastName', ''),
        "formatted_name": (user_bamboohr_data.get('firstName', '') + " " + user_bamboohr_data.get('middleName', '')).strip(),
        'startDate': rail.parse_date(user_bamboohr_data.get('hireDate'), config.DATE_FORMAT) if user_bamboohr_data.get('hireDate') else {},
        'job_run_date': dag_run.conf['job_run_date'],
        'log_timestamp': dag_run.conf['log_timestamp'],
        "user_log": rail.result('create_user_log'),
        "project_log": rail.result('create_project_log')
    }

    if action == 'update':
        conf.update({
            'useruri': rail.result('get_user_data')['userDetails']['uri'],
            "currentPayRule": rail.result('get_current_payrule_for_user'),
            "user_details_artifact": rail.result('get_user_details_artifact'),
        })

    return conf


def get_create_new_supervisor_payload(supervisor_details_from_bamboo, dag_run):
    return {
        **supervisor_details_from_bamboo,
        "merged_first_middle_name": (supervisor_details_from_bamboo.get('firstName', '') + " " + supervisor_details_from_bamboo.get(
            'middleName', '')).strip(),
        "supervisor_permission_set_uri": dag_run.conf['supervisor_permission_set_uri'],
        "custom_field_uris": dag_run.conf['custom_field_uris'],
        "subordinate_details": {
            "id": dag_run.conf.get('id'),
            "supervisorId": dag_run.conf.get('supervisorId'),
            "supervisorEId": dag_run.conf.get('supervisorEId'),
            "supervisor": dag_run.conf.get('supervisor'),
            "customClientName": dag_run.conf.get('customClientName'),
            "customProjectName": dag_run.conf.get('customProjectName'),
            "customDirectIndirect": dag_run.conf.get('customDirectIndirect')
        },
        "job_run_date": dag_run.conf.get('job_run_date'),
        "log_timestamp": dag_run.conf.get('log_timestamp'),
        "user_log": dag_run.conf.get('user_log')
    }


def get_notification_preferences_payload(user_uri):
    return {
        "user": {
            "uri": user_uri,
            "loginName": null,
            "parameterCorrelationId": null
        },
        "preferences": {
            "notificationDeliveryPreferences": [
                {
                    "objectTypeUri": "urn:replicon:object-type:project",
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
                    "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:time-off",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                },
                {
                    "objectTypeUri": "urn:replicon:object-type:holiday",
                    "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                }
            ],
            "sharedDeliveryPreferenceOptionUris": [
                "urn:replicon:user-shared-delivery-preference-option:do-not-deliver-on-time-off",
                "urn:replicon:user-shared-delivery-preference-option:do-not-deliver-on-non-work-days"
            ]
        }
    }


def create_user_payload(dag_run):
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf.get('workEmail'),
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf.get('formatted_name'),
            "lastname":  dag_run.conf.get('lastName'),
            "emailAddress":  dag_run.conf.get('workEmail'),
            "employeeId":  dag_run.conf.get('employeeNumber'),
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [],
            "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
            "employmentDateRange": {
                "startDate": dag_run.conf.get('startDate'),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:replicon"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf.get('workEmail'),
                "SSOName": null,
                "password": "UZUMymw@123#"
            },
            "holidayCalendar": null,
            "timeOffPolicy": null,
            "permissionSets": [
                {
                    "uri": null,
                    "name": "End User"
                }
            ],
            "policySets": [
                {
                    "uri": null,
                    "name": "Time Punches with Distribution"
                },
                {
                    "uri": null,
                    "name": "Time Off"
                },
                {
                    "uri": null,
                    "name": "All Devices Access"
                }
            ],
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": {
                "uri": null,
                "name": "Supervisor"
            },
            "expenseApprovalPath": null,
            "timeOffApprovalPath": {
                "uri": null,
                "name": "Supervisor"
            },
            "customFieldValues": [],
            "assignedActivities": [],
            "timeZone": null,
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [],
            "divisionSchedule": [],
            "costCenterSchedule": [
                {
                    "costCenter": {
                        "uri": null,
                        "parentUri": null,
                        "name": "General Pay-rule"
                    },
                    "effectiveDate": dag_run.conf.get('startDate'),
                }
            ],
            "serviceCenterSchedule": [],
            "departmentGroupSchedule": [],
            "employeeTypeGroupSchedule": [],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": "Weekly starting on Monday"
                    },
                    "effectiveDate": null
                }
            ],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [],
            "displayNameParameter": null
        }
    }


def create_supervisor_payload(dag_run, date_format):
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf.get('workEmail'),
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf.get('merged_first_middle_name'),
            "lastname": dag_run.conf.get('lastName'),
            "emailAddress": dag_run.conf.get('workEmail'),
            "employeeId": dag_run.conf.get('employeeNumber'),
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [],
            "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
            "employmentDateRange": {
                "startDate": rail.parse_date(dag_run.conf.get('hireDate'), date_format),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:replicon"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf.get('workEmail'),
                "SSOName": null,
                "password": "UZUMymw@123#"
            },
            "holidayCalendar": null,
            "timeOffPolicy": null,
            "permissionSets": [
                {
                    "uri": null,
                    "name": "End User"
                },
                {
                    "uri": null,
                    "name": "Supervisor"
                }
            ],
            "policySets": [
                {
                    "uri": null,
                    "name": "Time Punches with Distribution"
                },
                {
                    "uri": null,
                    "name": "Time Off"
                },
                {
                    "uri": null,
                    "name": "All Devices Access"
                }
            ],
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": {
                "uri": null,
                "name": "Supervisor"
            },
            "expenseApprovalPath": null,
            "timeOffApprovalPath": {
                "uri": null,
                "name": "Supervisor"
            },
            "customFieldValues": [],
            "assignedActivities": [],
            "timeZone": null,
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [],
            "divisionSchedule": [],
            "costCenterSchedule": [
                {
                    "costCenter": {
                        "uri": null,
                        "parentUri": null,
                        "name": "General Pay-rule"
                    },
                    "effectiveDate": rail.parse_date(dag_run.conf.get('hireDate'), date_format),
                }
            ],
            "serviceCenterSchedule": [],
            "departmentGroupSchedule": [],
            "employeeTypeGroupSchedule": [],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": "Weekly starting on Monday"
                    },
                    "effectiveDate": null
                }
            ],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [],
            "displayNameParameter": null
        }
    }


def get_timeoff_types_to_assign(all_time_off_types, eligible_timeoff_types_mapper):
    final_timeoff_types_to_assign = []
    for mapper_entry in eligible_timeoff_types_mapper:
        timeoff_uri = rail.find_first_by_attr_and_get_attr(
            all_time_off_types, 'name', mapper_entry['timeoff'], 'uri')
        if timeoff_uri:
            final_timeoff_types_to_assign.append({
                "timeOffType": {
                    'uri': timeoff_uri,
                    'name': mapper_entry['timeoff']
                },
                "isTimeOffAllowedAgainstThisTimeOffType": "true",
                "applyDefaultTimeOffTypePolicy": "true",
                "defaultTimeOffTypePolicyEffectiveDate": null,
                "policySchedule": []
            })

    return final_timeoff_types_to_assign


def get_user_modifications_payload(dag_run):
    return {
        "target": {
            "uri": rail.result('create_user_9')['uri']
        },
        "modifications": {
            "payRuleSchedule": [{
                "dateRange": null,
                "item": {
                    "uri": null,
                    "name": rail.result('get_required_payrule_to_assign_14_19')
                }
            }],
            "holidayCalendarSchedule": [{
                "dateRange": null,
                "item": {
                    "uri": null,
                    "name": rail.result('get_required_holiday_calendar_timezone_to_assign_22')['holiday_calendar']
                }
            }],
            "timeZone":  {
                "value": {
                    "uri": null,
                    "IANAName": rail.result('get_required_holiday_calendar_timezone_to_assign_22')['time_zone']
                }
            },
            "locationSchedule": [{
                "dateRange": null,
                "item": {
                    "uri": null,
                    "parentUri": null,
                    "name": dag_run.conf["location"]
                }
            }],
            "timeOffTypes": [{
                "modificationOptionUri": "urn:replicon:collection-modification-option:replace",
                "items": get_timeoff_types_to_assign(rail.result('get_all_time_off_types_26'), rail.result('get_list_of_eligible_timeoff_types_from_mapper_27'))
            }]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }


def disable_user_payload(dag_run, date_format):
    user_bamboo_profile_term_date = dag_run.conf.get('terminationDate', '')
    termination_date = user_bamboo_profile_term_date if (
        user_bamboo_profile_term_date and user_bamboo_profile_term_date != "0000-00-00") else dag_run.conf.get('job_run_date', '')
    return {
        "user": {
            "uri": dag_run.conf.get('useruri', '')
        },
        "modifications": {
            "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [{
                        "effectiveDate": rail.parse_date(termination_date, date_format),
                        "costCenter": {
                            "name": "No Pay-rule"
                        }
                    }]
                }
            },
            "securitySettingsToApply": {
                "loginEnabled": "0"
            },
            "userDetailsToApply": {
                "employmentEndDate": {
                    "date": rail.parse_date(termination_date, date_format)
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_enable_user_payload(dag_run, config):
    return {
        "target": {
            "uri": dag_run.conf['useruri']
        },
        "template": null,
        "modifications": {
            "employmentDateRange": {
                "value": {
                    "startDate": dag_run.conf['startDate'],
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": "true"
                    }
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }


def get_supervisor_modifications_payload(dag_run):
    modifications = {
        "holidayCalendarSchedule": [{
            "dateRange": null,
            "item": {
                "uri": null,
                "name": rail.result('get_required_holiday_calendar_timezone_to_assign_22')['holiday_calendar']
            }
        }],
        "timeZone":  {
            "value": {
                "uri": null,
                "IANAName": rail.result('get_required_holiday_calendar_timezone_to_assign_22')['time_zone']
            }
        },
        "locationSchedule": [{
            "dateRange": null,
            "item": {
                "uri": null,
                "parentUri": null,
                "name": dag_run.conf["location"]
            }
        }],
        "timeOffTypes": [{
            "modificationOptionUri": "urn:replicon:collection-modification-option:replace",
            "items": get_timeoff_types_to_assign(rail.result('get_all_time_off_types_26'), rail.result('get_list_of_eligible_timeoff_types_from_mapper_27'))
        }]
    }

    if bool(rail.result('payrule_to_assign')):
        modifications.update({
            "payRuleSchedule": [{
                "dateRange": null,
                "item": {
                    "uri": null,
                    "name": rail.result('payrule_to_assign')
                }
            }]
        })

    return {
        "target": {
            "uri": rail.result('create_supervisor_user_9')['uri']
        },
        "modifications": modifications,
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }
