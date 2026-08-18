from datetime import date
from mammoet.user_import_v1.utils import custom_methods
import rail

null = None

# pylint: disable=unused-argument

def get_default_timesheet_approval_path():
    return {
        "uri": null,
        "name": "Mammoet Approval Path"
    }

def get_default_timeentry_approval_path():
    return {
        'name': 'Mammoet Approval Path'
    }

def get_custom_fields_to_add(dag_run, effective_date):

    if dag_run.conf['country'].lower() != "belgium":
        return [
            {
                "customField": {
                    "uri": dag_run.conf['custom_fields']['location']['uri'],
                },
                "text": dag_run.conf['country'],
                "date": null,
                "dropDownOption": null,
                "number": null,
            }
        ]
    return [
        {
            "customField": {
                "uri": dag_run.conf['custom_fields']['location']['uri'],
            },
            "text": dag_run.conf['country'],
            "date": null,
            "dropDownOption": null,
            "number": null,
        },
        {
            "customField": {
                "uri": dag_run.conf['custom_fields']['voluntary_ot_effective_date']['uri'],
            },
            "text": null,
            "date":effective_date,
            "dropDownOption": null,
            "number": null,
        },
        {
            "customField": {
                "uri": dag_run.conf['custom_fields']['voluntary_ot']['uri'],
            },
            "text": null,
            "date": null,
            "dropDownOption": {
                "uri": null,
                "name": dag_run.conf['voluntary_ot']
            },
            "number": null,
        }
    ]

def get_location_to_add_payload(dag_run, effective_date):
    return [
        {
            "location": {
                "uri": dag_run.conf['groups']['location']['uri'],
                "parentUri": null,
                "name": null,
            },
            "effectiveDate":effective_date,
        }
    ]

def get_legal_entity_to_add_payload(dag_run, effective_date):
    return [
        {
            "departmentGroup": {
                "uri": dag_run.conf['groups']['legal_entities']['uri'],
                "parent": null,
                "name": null,
                "parameterCorrelationId": null,
            },
            "effectiveDate":effective_date,
        }
    ]

def get_cost_center_to_add_payload(dag_run, effective_date):
    return [
        {
            "costCenter": {
                "uri": dag_run.conf['groups']['cost_center']['uri'],
                "parentUri": null,
                "name": null,
            },
            "effectiveDate":effective_date,
        }
    ]

def get_employee_type_to_add_payload(dag_run, effective_date):
    return [
        {
            "employeeTypeGroup": {
                "uri": dag_run.conf['groups']['employee_type']['uri'],
                "parent": null,
                "name": null,
                "parameterCorrelationId": null,
            },
            "effectiveDate":effective_date,
        }
    ]

def get_pay_grade_to_add_payload(dag_run, effective_date):
    return [
        {
            "serviceCenter": {
                "uri": dag_run.conf['groups']['pay_grade']['uri'],
                "parentUri": null,
                "name": null,
            },
            "effectiveDate":effective_date,
        }
    ]

def get_permission_to_assign(dag_run):
    if dag_run.conf['is_supervisor'].lower() == "no":
        return [{
                "uri": dag_run.conf['user_permissions']['basic']['uri'],
                "name": null,
            }]
    return [
        {
            "uri": dag_run.conf['user_permissions']['basic']['uri'],
            "name": null,
        },
        {
            "uri": dag_run.conf['user_permissions']['supervisor']['uri'],
            "name": null,
        }
    ]

def get_policies_to_assign(dag_run):
    policies =[
        {
            "uri": dag_run.conf['user_templates']['timeoff_template']['uri'],
            "name": null,
        }
    ]
    if dag_run.conf['user_templates']['timesheet_template']:
        policies.append(
            {
                'uri': dag_run.conf['user_templates']['timesheet_template']['uri'],
                'name': null
            }
        )
    return policies

def get_schedule_policy(dag_run, effective_date, logger):
    if not dag_run.conf['replicon_office_schedule']:
        logger.append('schedule not found in replicon')
        return []

    return [
      {
        "schedulePolicy": {
          "officeScheduleUri": null,
          "name": dag_run.conf['office_schedule_name'],
          "officeSchedule": null,
          "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
        },
        "effectiveDate": effective_date
      }
    ]

def get_payrule_to_assign(dag_run, effective_date, logger):
    if not dag_run.conf['replicon_payrule_scripts']:
        logger.append('Payrule not found in replicon')
        return []
    return [
      {
        "payRuleScript": {
          "uri": null,
          "name": dag_run.conf['payrule_name']
        },
        "effectiveDate": effective_date
      }
    ]

def get_create_user_payload(dag_run):
    effective_date = custom_methods.get_replicon_date_from_str(
                            dag_run.conf['group_effective_start_date']if dag_run.conf['group_effective_start_date'] else  dag_run.conf['start_date'])
    voluntary_ot_effective_date =  custom_methods.get_replicon_date_from_str(
        dag_run.conf['voluntary_ot_effective_date']) if dag_run.conf['voluntary_ot_effective_date'] else null
    logger = []
    exception = bool((not dag_run.conf['replicon_payrule_scripts']) or (not dag_run.conf['replicon_office_schedule']))
    payload = {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf["login_name"],
                "employeeId": null,
                "parameterCorrelationId": null,
            },
            "firstname": dag_run.conf['first_name'],
            "lastname": dag_run.conf['last_name'],
            "emailAddress": dag_run.conf['email_id'],
            "employeeId": dag_run.conf['employee_id'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": get_schedule_policy(dag_run, effective_date, logger) if dag_run.conf['office_schedule_name'] else [],
            "workWeekStartDayUri": dag_run.conf['mapper_derived']['work_week'].get('uri', null),
            "employmentDateRange": {
                "startDate": custom_methods.get_replicon_date_from_str(dag_run.conf['start_date']), # mandatory for new users
                "endDate": custom_methods.get_replicon_date_from_str(dag_run.conf['end_date']) if dag_run.conf['end_date'] else null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "0" if "indirect office" in dag_run.conf["employee_type_name"].lower() else "1",
                "loginName": dag_run.conf["login_name"],
                "SSOName": dag_run.conf["login_name"],
                "password": null,
            },
            "holidayCalendar": {
                "uri": dag_run.conf['holiday_calender'].get('uri'),
                "name": null
            } if dag_run.conf['holiday_calender'].get('uri') else null,
            "timeOffPolicy": null,
            "permissionSets": get_permission_to_assign(dag_run),
            "policySets": get_policies_to_assign(dag_run),
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": get_default_timesheet_approval_path(),
            "expenseApprovalPath": null,
            "timeOffApprovalPath": null,
            "workAuthorizationApprovalPath": null,
            "customFieldValues": get_custom_fields_to_add(dag_run, voluntary_ot_effective_date),
            "assignedActivities": list(map(
                lambda activity: {'uri': activity['uri']}, dag_run.conf['activities']['activities'])) if dag_run.conf['activities']['activities'] else [],
            "timeZone":{
                "uri": dag_run.conf['mapper_derived']['timezone']['uri'],
                "IANAName": null
            } if dag_run.conf['mapper_derived']['timezone'] else null,
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": get_location_to_add_payload(dag_run, effective_date) if dag_run.conf['location'] else [],
            "divisionSchedule": [],
            "costCenterSchedule": get_cost_center_to_add_payload(dag_run, effective_date) if dag_run.conf['cost_center'] else [],
            "serviceCenterSchedule": get_pay_grade_to_add_payload(dag_run, effective_date) if dag_run.conf['pay_grade_name'] else [],
            "departmentGroupSchedule": get_legal_entity_to_add_payload(dag_run, effective_date) if dag_run.conf['legal_entity'] else [],
            "employeeTypeGroupSchedule": get_employee_type_to_add_payload(dag_run, effective_date) if dag_run.conf['employee_type_name'] else [],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": dag_run.conf['mapper_derived']['timesheet_period'].get('TimesheetPeriod')
                    },
                    "effectiveDate": custom_methods.get_replicon_date_from_str(dag_run.conf['start_date'])
                }
            ] if dag_run.conf['mapper_derived']['timesheet_period'] else [],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": get_payrule_to_assign(dag_run, effective_date, logger) if dag_run.conf['payrule_name'] else [],
            "displayNameParameter": null,
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": [],
        }
    }

    if dag_run.conf['location']:
        logger.append("Location Assigned")
    if dag_run.conf['cost_center']:
        logger.append("Cost Center Assigned")
    if dag_run.conf['pay_grade_name']:
        logger.append("Pay Grade Assigned")
    if dag_run.conf['legal_entity']:
        logger.append("Legal Entity Assigned")
    if dag_run.conf['employee_type_name']:
        logger.append("Employee Type Assigned")
    if dag_run.conf['payrule_name'] and dag_run.conf['replicon_payrule_scripts']:
        logger.append("Payrule Assigned")
    if dag_run.conf['holiday_calender'].get('uri'):
        logger.append("Holiday Calender Assigned")
    else:
        logger.append("Holiday Calender is not assigned")
    rail.set_result(key="log", val=rail.smartjoin_by_delim(logger, ';'))
    rail.set_result(key='has_exception', val=exception)
    return payload


def get_indirect_add_user_payload(dag_run):
    effective_date = custom_methods.get_replicon_date_from_str( dag_run.conf['start_date'])
    payload = {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf["login_name"],
                "employeeId": null,
                "parameterCorrelationId": null,
            },
            "firstname": dag_run.conf['first_name'],
            "lastname": dag_run.conf['last_name'],
            "emailAddress": dag_run.conf['email_id'],
            "employeeId": dag_run.conf['employee_id'],
            "employmentDateRange": {
                "startDate": custom_methods.get_replicon_date_from_str(dag_run.conf['start_date']), # mandatory for new users
                "endDate": custom_methods.get_replicon_date_from_str(dag_run.conf['end_date']) if dag_run.conf['end_date'] else null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "0",
                "loginName": dag_run.conf["login_name"],
                "SSOName": dag_run.conf["login_name"],
                "password": null,
            },
            "locationSchedule": get_location_to_add_payload(dag_run, effective_date) if dag_run.conf['location'] else [],
            "divisionSchedule": [],
            "costCenterSchedule": get_cost_center_to_add_payload(dag_run, effective_date) if dag_run.conf['cost_center'] else [],
            "serviceCenterSchedule": get_pay_grade_to_add_payload(dag_run, effective_date) if dag_run.conf['pay_grade_name'] else [],
            "departmentGroupSchedule": get_legal_entity_to_add_payload(dag_run, effective_date) if dag_run.conf['legal_entity'] else [],
            "employeeTypeGroupSchedule": get_employee_type_to_add_payload(dag_run, effective_date) if dag_run.conf['employee_type_name'] else [],
        }
    }
    return payload

def is_group_changed(dag_run, group):
    return rail.result('get_effectivegroup_membership').get(group, {}).get('uri', '') != dag_run.conf['groups'][group]['uri']

def get_location_update_payload(dag_run, effective_date, logger):
    if not effective_date:
        return null

    if is_group_changed(dag_run, 'location'):
        logger.append("Location updated")
        return {
            "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementLocationSchedule": [],
            "updateLocationScheduleOverDateRange": {
                "replacementLocationScheduleEntries": [
                    {
                        "location": {
                        "uri": dag_run.conf['groups']['location']['uri'],
                        "parentUri": null,
                        "name": null
                        },
                        "effectiveDate": effective_date
                    }
                ],
                "endDate": null
            }
        }

    return null


def get_paygrade_update_payload(dag_run, effective_date, logger):
    if not effective_date:
        return null

    if is_group_changed(dag_run, 'pay_grade'):
        logger.append('Pay Grade updated')
        return {
            "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementServiceCenterSchedule": [],
            "updateServiceCenterScheduleOverDateRange": {
                "replacementServiceCenterScheduleEntries": [
                    {
                        "serviceCenter": {
                            "uri": dag_run.conf['groups']['pay_grade']['uri'],
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": effective_date
                    }
                ],
                "endDate": null
            }
        }
    return null

def get_cost_center_to_update_payload(dag_run, effective_date, logger):
    if not effective_date:
        return null
    if is_group_changed(dag_run, 'cost_center'):
        logger.append('Cost center updated')
        return {
            "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementCostCenterSchedule": [],
            "updateCostCenterScheduleOverDateRange": {
                "replacementCostCenterScheduleEntries": [
                    {
                        "costCenter": {
                            "uri": dag_run.conf['groups']['cost_center']['uri'],
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": effective_date
                    }
                ],
                "endDate": null
            }
        }
    return null

def get_employee_type_to_update_payload(dag_run, effective_date, logger):
    if not effective_date:
        return null
    if is_group_changed(dag_run, 'employee_type'):
        logger.append('Employee type updated')
        return {
            "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementEmployeeTypeGroupSchedule": [],
            "updateEmployeeTypeGroupScheduleOverDateRange": {
                "replacementEmployeeTypeGroupScheduleEntries": [
                    {
                        "employeeTypeGroup": {
                            "uri":  dag_run.conf['groups']['employee_type']['uri'],
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": effective_date
                    }
                ],
                "endDate": null
            }
        }
    return null


def get_legal_entity_to_update_payload(dag_run, effective_date, logger):
    if not effective_date:
        return null
    if is_group_changed(dag_run, 'legal_entities'):
        logger.append('Legal entity updated')
        return {
            "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementDepartmentGroupSchedule": [],
            "updateDepartmentGroupScheduleOverDateRange": {
                "replacementDepartmentGroupScheduleEntries": [
                    {
                        "departmentGroup": {
                        "uri": dag_run.conf['groups']['legal_entities']['uri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                        },
                        "effectiveDate": effective_date
                    }
                ],
                "endDate": null
            }
        }
    return null

def get_custom_fields_to_update(user_details, dag_run, effective_date, logger):
    user_custom_fields = user_details['userDetails']['customFieldValues']
    custom_fields_to_update = []

    if rail.find_first_by_attr_and_get_attr(user_custom_fields, 'customField.displayText', 'User Country', 'text') != dag_run.conf['country']:
        custom_fields_to_update.append({
            "customField": {
                "uri": dag_run.conf['custom_fields']['location']['uri'],
            },
            "text": dag_run.conf['country'],
            "date": null,
            "dropDownOption": null,
            "number": null,
        })

    if dag_run.conf['country'].lower() == "belgium":
        can_update_ot = rail.find_first_by_attr_and_get_attr(
            user_custom_fields, 'customField.displayText', 'Voluntary OT', 'text') != dag_run.conf['voluntary_ot']
        if effective_date and rail.find_first_by_attr_and_get_attr(
                user_custom_fields, 'customField.displayText', 'Voluntary OT Effective Date', 'date') != effective_date:
            custom_fields_to_update.append({
                "customField": {
                    "uri": dag_run.conf['custom_fields']['voluntary_ot_effective_date']['uri'],
                },
                "text": null,
                "date":effective_date,
                "dropDownOption": null,
                "number": null,
            })

        if can_update_ot:
            custom_fields_to_update.append({
                "customField": {
                    "uri": dag_run.conf['custom_fields']['voluntary_ot']['uri'],
                },
                "text": null,
                "date": null,
                "dropDownOption": {
                    "uri": null,
                    "name": dag_run.conf['voluntary_ot']
                },
                "number": null,
            })

    return custom_fields_to_update


def convert_json_date_to_str(json_date):
    return f"{json_date['day']}/{json_date['month']}/{json_date['year']}"

def get_user_details_to_update(dag_run, user_details, logger):
    user_details_to_apply = {}
    if dag_run.conf['first_name'] != user_details['firstName']:
        user_details_to_apply['firstName'] = dag_run.conf['first_name']

    if dag_run.conf['last_name'] !=  user_details['lastName']:
        user_details_to_apply['lastName'] = dag_run.conf['last_name']

    if dag_run.conf['start_date']:
        if not user_details['employmentDateRange']['startDate'] or (not custom_methods.is_both_date_are_same(
                                        dag_run.conf['start_date'], convert_json_date_to_str(user_details['employmentDateRange']['startDate']))):
            user_details_to_apply['employmentStartDate'] = {
                'date': custom_methods.get_replicon_date_from_str(dag_run.conf['start_date'])
            }

    if dag_run.conf['end_date']:
        if not user_details['employmentDateRange']['endDate'] or (not custom_methods.is_both_date_are_same(
                                        dag_run.conf['end_date'], convert_json_date_to_str(user_details['employmentDateRange']['endDate']))):
            user_details_to_apply['employmentEndDate'] = {
                'date': custom_methods.get_replicon_date_from_str(dag_run.conf['end_date'])
            }

    if dag_run.conf['email_id']:
        if user_details['emailAddress'] != dag_run.conf['email_id']:
            user_details_to_apply["emailAddress"] = {
                "emailAddress": dag_run.conf['email_id']
            }

    return user_details_to_apply if user_details_to_apply else null

def get_policies_to_assign_update(dag_run, logger):
    if dag_run.conf['user_templates']['timesheet_template']:
        return {
            "policySetUrisToAssign": [
                dag_run.conf['user_templates']['timesheet_template']['uri']
            ],
            "policyUrisToRemovePolicySet": []
        }
    return null

def get_activities_to_apply(dag_run, logger):
    if dag_run.conf['activities']['exception']:
        logger.append(rail.smartjoin_by_delim(dag_run.conf['activities']['exception']))

    if not dag_run.conf['activities']['activities']:
        return null

    return {
      "activitiesToAssign": list(map(lambda activity: {'uri': activity['uri']}, dag_run.conf['activities']['activities'])),
      "activitiesToRemove": []
    }

def get_schedule_to_update(dag_run, effective_date, log, user_details):
    if not dag_run.conf['replicon_office_schedule']:
        log.append("Schedule not found in replicon")
        return null

    if user_details['schedulePolicies'] and user_details['schedulePolicies'][-1].get(
        'officeSchedule', {}).get('uri') == dag_run.conf['replicon_office_schedule']['uri']:
        return null
    return {
      "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
      "replacementSchedule": [],
      "updateScheduleOverDateRange": {
        "replacementScheduleEntries": [
          {
            "schedulePolicy": {
              "officeScheduleUri": null,
              "name": null,
              "officeSchedule": {
                "officeScheduleUri": dag_run.conf['replicon_office_schedule']['uri'],
                "name": null
              },
              "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
            },
            "effectiveDate": effective_date
          }
        ],
        "endDate": null
      }
    }

def get_pay_rule_update_payload(dag_run, user_details, log, effective_date):
    if dag_run.conf['payrule_name'] and not dag_run.conf['replicon_payrule_scripts']:
        log.append("Payrule not found in Replicon")
        return null

    if not user_details['payRuleScriptSchedule']:
        return {
            "scheduleEntries": [
                {
                "payRuleScript": {
                    "uri": dag_run.conf['replicon_payrule_scripts']['uri'],
                    "name": null
                },
                "effectiveDate": effective_date
                }
            ]
        }

    if user_details['payRuleScriptSchedule'] and \
        user_details['payRuleScriptSchedule'][-1]['payRuleScript']['uri'] != dag_run.conf['replicon_payrule_scripts']['uri']:
        return null

    return {
      "scheduleEntries": [
        {
          "payRuleScript": {
            "uri": dag_run.conf['replicon_payrule_scripts']['uri'],
            "name": null
          },
          "effectiveDate": effective_date
        }
      ]
    }


def get_holiday_calendar_update_payload(dag_run, user_details, logger):
    if not dag_run.conf['holiday_calender'].get('uri'):
        return null
    if user_details['holidayCalendar']:
        if dag_run.conf['holiday_calender'].get('uri') == user_details['holidayCalendar']['uri']:
            return null
    logger.append("Holiday Calendar updated")
    return {
        "holidayCalendar": {
            "uri": dag_run.conf['holiday_calender'].get('uri'),
            "name": null
        }
    }


def get_update_user_payload(dag_run):
    effective_date = custom_methods.get_replicon_date_from_str(
        dag_run.conf['group_effective_start_date']) if dag_run.conf['group_effective_start_date'] else null
    voluntary_ot_effective_date =  custom_methods.get_replicon_date_from_str(
        dag_run.conf['voluntary_ot_effective_date']) if dag_run.conf['voluntary_ot_effective_date'] else null
    user_details = rail.result('get_user_details')[0]
    logger = []
    payload = {
        "user": {
            "uri": user_details['userDetails']['uri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "timezoneToApply": {
                "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": {
                    "uri": dag_run.conf['mapper_derived']['timezone']['uri'],
                    "IANAName": null
                }
            } if dag_run.conf['mapper_derived']['timezone'] else null,
            "workWeekStartToApply": get_update_work_week_payload(dag_run, user_details),
            "holidayCalendarToApply": get_holiday_calendar_update_payload(dag_run, user_details, logger),
            "holidayCalendarAssignmentsToApply": null,
            "schedulePolicyToApply": get_schedule_to_update(dag_run, effective_date, logger, user_details),
            "locationScheduleToApply": get_location_update_payload(dag_run, effective_date, logger) if dag_run.conf['location'] else null,
            "divisionScheduleToApply": null,
            "costCenterScheduleToApply": get_cost_center_to_update_payload(dag_run, effective_date, logger) if dag_run.conf['cost_center'] else null,
            "departmentGroupScheduleToApply": get_legal_entity_to_update_payload(dag_run, effective_date, logger) if dag_run.conf['legal_entity'] else null,
            "employeeTypeGroupScheduleToApply": get_employee_type_to_update_payload(
                dag_run, effective_date, logger) if dag_run.conf['employee_type_name'] else null,
            "timesheetPeriodScheduleToApply": get_timesheet_period_update_payload(dag_run, user_details, logger),
            "serviceCenterScheduleToApply": get_paygrade_update_payload(dag_run, effective_date, logger) if dag_run.conf['pay_grade_name'] else null,
            "totalBusinessCostScheduleToApply": null,
            "permissionSetsToApply": null,
            "policySetsToApply": get_policies_to_assign_update(dag_run, logger),
            "policyDataAccessScopesToApply": null,
            "policyDataAccessScopesToApply2": null,
            "notificationPreferencesToApply": null,
            "timesheetPeriodTypeToApply": null,
            "timesheetApprovalPathToApply": get_default_timesheet_approval_path(),
            "timeEntryRevisionGroupApprovalPathToApply": get_default_timeentry_approval_path(),
            "validationRuleToApply": null,
            "activitiesToApply2": get_activities_to_apply(dag_run, logger),
            "defaultActivityToApply": null,
            "defaultActivityToApply2": null,
            "defaultTimeOffTypeForBookingsToApply": null,
            "expenseApprovalPathToApply": null,
            "timeOffApprovalPathToApply": null,
            "productAssignmentsToApply": null,
            "timeBankPolicyToApply": null,
            "securitySettingsToApply": null,
            "supervisorsToApply": null,
            "supervisorsModifications": null,
            "payrollRatesToApply": null,
            "payrollRatesModifications": null,
            "overtimeRulesToApply": null,
            "overtimeRulesModifications": null,
            "customFieldValuesToApply": get_custom_fields_to_update(
                user_details, dag_run, voluntary_ot_effective_date, logger),
            "departmentToApply": null,
            "employeeTypeToApply": null,
            "userDetailsToApply": get_user_details_to_update(dag_run, user_details['userDetails'], logger),
            "payRulesToApply": null,
            "payRulesScheduleModifications": null,
            "payRatesModifications": null,
            "placeAssignmentsModifications": null,
            "resourceAllocationAfterUserEndDateOptionUri": null,
            "projectRolesToApply": null,
            "projectRoleAssignmentSchedulesToApply": null,
            "decimalSeparatorToApply": null,
            "numberGroupSeparatorToApply": null,
            "dateFormatToApply": null,
            "clockFormatToApply": null,
            "hoursFormatToApply": null,
            "timeZoneFormatToApply": null,
            "objectExtensionFieldsToApply": [],
            "costRateScheduleModifications": null,
            "workAuthorizationApprovalPathToApply": null,
            "displayNameFormatSettingsToApply": null,
            "timePunchTimeZoneDisplayOptionToApply": null,
            "defaultTimesheetToDisplayOptionToApply": null,
            "reportSettingsToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }
    rail.set_result(key='log', val=rail.smartjoin_by_delim(logger, ';'))
    return payload



def update_user_end_date_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['user_uri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "userDetailsToApply": {
                "employmentEndDate": {
                    "date": custom_methods.get_replicon_date_from_str(dag_run.conf['end_date'])
                },
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_update_work_week_payload(dag_run, user_details):
    current_work_week_uri = user_details['userDetails'].get('workWeekStartDay', {}).get('uri', null)
    new_work_week_uri = dag_run.conf['mapper_derived']['work_week'].get('uri', null)
    if not current_work_week_uri:
        return {
            "workWeekStartDayUri": new_work_week_uri
        }
    if current_work_week_uri != new_work_week_uri:
        return {
            "workWeekStartDayUri": new_work_week_uri
        }
    return null

def is_date_less_than_today(effective_date, _todays_date):
    return date(year=effective_date['year'], month=effective_date['month'], day=effective_date['day']) < _todays_date

def get_timesheet_period_update_payload(dag_run, user_details, logger):
    _todays_date = custom_methods.get_today_date(return_as_date=True)
    users_assigned_timesheet_periods = list(
        filter(lambda item: is_date_less_than_today(item['effectiveDate'],_todays_date), user_details['timesheetPeriodSchedule']))
    users_assigned_timesheet_periods = users_assigned_timesheet_periods[-1] if users_assigned_timesheet_periods else {}
    if users_assigned_timesheet_periods:
        if users_assigned_timesheet_periods['timesheetPeriod'] == dag_run.conf['mapper_derived']['timesheet_period'].get('TimesheetPeriod'):
            return null
    logger.append("Timesheet period updated")
    return {
        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementTimesheetPeriodSchedule": [],
        "updateTimesheetPeriodScheduleOverDateRange": {
            "replacementTimesheetPeriodScheduleEntries": [
            {
                "timesheetPeriod": {
                "uri": null,
                "name": dag_run.conf['mapper_derived']['timesheet_period'].get('TimesheetPeriod')
                },
                "effectiveDate": custom_methods.get_today_date()
            }
            ],
            "endDate": null
        }
    }
