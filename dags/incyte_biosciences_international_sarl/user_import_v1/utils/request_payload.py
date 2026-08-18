from datetime import datetime, timedelta
import json
import ast
import uuid
import rail


null= None
DATE_FORMAT = "%d/%m/%Y"

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_replicon_date(date_str):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, DATE_FORMAT)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None

MANDATORY_FIELDS = {
        "first_name":"First Name",
        "last_name": "Last Name",
        "start_date": "Start Date",
        "login_name": "EE Login",
        "country_name": "Country",
        "country_code": "Countrys Code"
}

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")

def get_location_payload():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:location-list-filter:effectively-enabled"
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
        }
    }

def get_add_department_payload(dag_run):
    return {
        "departmentGroup": {
            "parent": {
                "uri": rail.result("get_parent_department_details")[0]['uri']
            },
        },
        "modifications": {
            "name": dag_run.conf['department_name'],
            "isEnabled": "1"
        },
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_all_employee_grp_payload():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:employee-type-group"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:employee-type-group-list-filter:effectively-enabled"
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
        }
    }

def get_costcenter_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:cost-center-list-filter:effectively-enabled"
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
        }
    }

def test_valid_fields(dag_run):
    startdate = get_replicon_date(dag_run.conf['start_date'])
    if not startdate:
        return False
    if dag_run.conf['end_date']:
        enddate = get_replicon_date(dag_run.conf['end_date'])
        if not enddate:
            return False
    return True

def get_invalid_fields_message(dag_run):
    log=[]
    startdate = get_replicon_date(dag_run.conf['start_date'])
    if not startdate:
        log.append('Invalid format for Start Date')
    if dag_run.conf['end_date']:
        enddate = get_replicon_date(dag_run.conf['end_date'])
        if not enddate:
            log.append('Invalid format for End Date')
    return rail.smartjoin_by_delim(log,";")

def get_process_users_conf(item, config):
    policy_sets = rail.result('get_all_policy_sets')
    timesheet_approval_paths = rail.result('get_timesheet_approval_paths')
    holiday_calenders = rail.result('get_all_holiday_calenders')
    schedules = rail.result('get_all_office_schedules')

    def get_specific_details(country_name, all_data):
        data =list(filter(lambda x:str(x['name']).startswith(country_name) and not str(x['name']).startswith(country_name+'_Time Off Template'),
                map(lambda item: {
            'name': item['displayText'],
            'uri': item['uri']
        }, all_data)))
        if not data:
            return []
        return{
            "name": data[0]['name'],
            'uri': data[0]['uri']
        }

    def get_time_off_template_details(country_name, all_data):
        data =list(filter(lambda x:str(x['name']).startswith(country_name +'_Time Off Template'), map(lambda item: {
            'name': item['displayText'],
            'uri': item['uri']
        }, all_data)))
        if not data:
            return []
        return{
            "name": data[0]['name'],
            'uri': data[0]['uri']
        }

    def get_payrule_details(country_name):
        data =list(filter(lambda x:str(x['name']).startswith('*Incyte_'+country_name), map(lambda item: {
            'name': item['displayText'],
            'uri': item['uri']
        }, rail.result('get_all_payrule_scripts'))))
        if not data:
            return []
        return{
            "name": data[0]['name'],
            'uri': data[0]['uri']
        }

    return {
        **dict(item.items()),
        **{
            'country_grp_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_countries_grp'), 'name', item['country_name'], 'uri'),
            'department_grp_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_departments_grp'), 'full_path', item['dept_full_path'], 'uri'),
            'standard_hours_grp_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_standard_hours_grp'), 'name', item['standard_hours'], 'uri'),
            'full_part_time_grp_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_full_part_time_grp'), 'name', item['full_part_time'], 'uri'),
            'work_location_grp_uri':rail.find_first_by_attr_and_get_attr(
                rail.result('get_updated_work_location_grp'), 'name', item['work_location_name'], 'uri'),
            'employee_type_grp_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_updated_employee_types_grp'), 'name', item['employee_type'], 'uri'),
            'business_title_definition_uri': rail.result('get_user_udfs')['business_title_definition_uri'],
            'fte_definition_uri': rail.result('get_user_udfs')['fte_definition_uri'],
            'hr_manager_id_definition_uri': rail.result('get_user_udfs')['hr_manager_id_definition_uri'],
            'hr_manager_id_dropdown_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_updated_hr_manager_udf_dropdown_values'),
                'name', item['hr_manager_id'], 'uri'),
            'basic_user_permission_uri':  rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'),
                'displayText','Basic User with Reports','uri'),
            'supervisor_permission_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'),
                'displayText','Supervisor','uri'),
            'timesheet_template_name': get_specific_details(item['country_name'], policy_sets)['name']
                if get_specific_details(item['country_name'], policy_sets) else null,
            'timesheet_template_uri': get_specific_details(item['country_name'], policy_sets)['uri']
                if get_specific_details(item['country_name'], policy_sets) else null,
            'timesheet_approval_path_name': get_specific_details(item['country_name'], timesheet_approval_paths)['name']
                if get_specific_details(item['country_name'], timesheet_approval_paths) else null,
            'timesheet_approval_path_uri': get_specific_details(item['country_name'], timesheet_approval_paths)['uri']
                if get_specific_details(item['country_name'],  timesheet_approval_paths) else null,
            'timesheet_period': config.TIMESHEET_PERIOD,
            'timeoff_template_name': get_time_off_template_details(item['country_name'], policy_sets)['name']
                if get_time_off_template_details(item['country_name'], policy_sets) else null,
            'timeoff_template_uri': get_time_off_template_details(item['country_name'], policy_sets)['uri']
                if get_time_off_template_details(item['country_name'], policy_sets) else null,
            'holiday_calender_name':get_specific_details(item['country_name'], holiday_calenders)['name']
                if get_specific_details(item['country_name'], holiday_calenders) else null,
            'holiday_calender_uri': get_specific_details(item['country_name'], holiday_calenders)['uri']
                if get_specific_details(item['country_name'], holiday_calenders) else null,
            'schedule_name': get_specific_details(item['country_name'], schedules)['name'] if get_specific_details(item['country_name'], schedules)
                else null,
            'schedule_uri': get_specific_details(item['country_name'], schedules)['uri'] if get_specific_details(item['country_name'], schedules)
                else null,
            'payrule_name': get_payrule_details(item['country_name'])['name'] if get_payrule_details(item['country_name'])
                else null,
            'payrule_uri': get_payrule_details(item['country_name'])['uri'] if get_payrule_details(item['country_name'])
                else null,
            'supervisor_log' : rail.result('create_supervisor_log'),
        }
    }

def get_process_new_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'user_log' : rail.result('create_user_log')
        }
    }

def get_process_update_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'user_log': rail.result('create_user_log'),
            'useruri': rail.result('get_user_data')[0]['uri'],
            'todays_date': (datetime.now()).strftime(DATE_FORMAT)
        }
    }

def get_remove_timeoff_payload():
    return {
        "userUri": rail.result('add_new_user')['uri'],
        "timeOffTypeUris": []
    }

def get_udfs(userstatus, dag_run):
    # pylint: disable=too-many-branches
    udfs = []
    def add_udf_field_values(definitionuri, dropdownuri = null, textvalue = null , number = null):
        udfs.append({
        "customField": {
          "uri": definitionuri,
          "name": null,
          "groupUri": null
        },
        "text": textvalue,
        "date": null,
        "dropDownOption": {
          "uri": dropdownuri,
          "name": null
        } if dropdownuri != null else null,
        "number": number
      })

    if userstatus =='adduser':
        if dag_run.conf['business_title']:
            add_udf_field_values(definitionuri = dag_run.conf['business_title_definition_uri'], textvalue= dag_run.conf['business_title'])
        if dag_run.conf['fte']:
            add_udf_field_values(definitionuri = dag_run.conf['fte_definition_uri'], textvalue= dag_run.conf['fte'])
        if dag_run.conf['hr_manager_id']:
            add_udf_field_values(definitionuri = dag_run.conf['hr_manager_id_definition_uri'], dropdownuri= dag_run.conf['hr_manager_id_dropdown_uri'])


    if userstatus =='updateuser':
        current_buissness_title = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Business Title', 'text')
        if dag_run.conf['business_title'] and current_buissness_title != dag_run.conf['business_title']:
            add_udf_field_values(definitionuri = dag_run.conf['business_title_definition_uri'], textvalue= dag_run.conf['business_title'])

        current_fte = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'FTE%', 'text')
        if dag_run.conf['fte'] and current_fte != dag_run.conf['fte']:
            add_udf_field_values(definitionuri = dag_run.conf['fte_definition_uri'], textvalue= dag_run.conf['fte'])

        current_hr_manager = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'HR Manager', 'text')
        if dag_run.conf['hr_manager_id'] and current_hr_manager != dag_run.conf['hr_manager_id']:
            add_udf_field_values(definitionuri = dag_run.conf['hr_manager_id_definition_uri'], dropdownuri= dag_run.conf['hr_manager_id_dropdown_uri'])

    return udfs


def get_put_user_payload(dag_run):
    def get_policy_sets_for_new_user(dag_run):
        policy_set=[]
        if dag_run.conf['timesheet_template_uri']:
            policy_set.append({
                    "uri": dag_run.conf['timesheet_template_uri'],
                    "name": null
                })
        if dag_run.conf['timeoff_template_uri']:
            policy_set.append({
                    "uri": dag_run.conf['timeoff_template_uri'],
                    "name": null
                })
        if not policy_set:
            return null
        return policy_set

    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['login_name'],
            },
            "firstname": dag_run.conf['first_name'],
            "lastname": dag_run.conf['last_name'],
            "emailAddress": dag_run.conf['email_id'] if dag_run.conf['email_id'] else null,
            "employeeId": dag_run.conf['employee_id'] if dag_run.conf['employee_id'] else null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": dag_run.conf['schedule_uri'],
                        "name": null,
                        "officeSchedule": {
                            "officeScheduleUri":dag_run.conf['schedule_uri'],
                            "name": null
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['schedule_uri'] else
            [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": "8 hours/day; Mon-Fri",
                        "officeSchedule": {
                            "officeScheduleUri":null,
                            "name": "8 hours/day; Mon-Fri"
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ],
            "employmentDateRange": {
                "startDate": get_replicon_date(dag_run.conf['start_date']),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                   "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['login_name'],
                "SSOName": dag_run.conf['login_name'],
            },
            "holidayCalendar": {
                "uri": dag_run.conf['holiday_calender_uri'],
                "name": null
            } if dag_run.conf['holiday_calender_uri'] else null,
            "permissionSets": [
                {
                    "uri": dag_run.conf['basic_user_permission_uri'],
                    "name": null
                }
            ],
            "policySets": get_policy_sets_for_new_user(dag_run),
            "timesheetApprovalPath": {
                "uri": dag_run.conf['timesheet_approval_path_uri'] if dag_run.conf['timesheet_approval_path_uri'] else null,
                "name": null if dag_run.conf['timesheet_approval_path_uri'] else 'System Approval'
                } ,
            "customFieldValues": get_udfs('adduser', dag_run),
            "assignedActivities": [],
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [
                {
                    "location": {
                        "uri": dag_run.conf['country_grp_uri'],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['country_grp_uri'] else [],
            "divisionSchedule":  [
                {
                    "division": {
                    "uri": dag_run.conf['work_location_grp_uri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
                ] if dag_run.conf['work_location_grp_uri'] else [],
            "costCenterSchedule": [
                {
                    "costCenter": {
                    "uri": dag_run.conf['full_part_time_grp_uri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
                ] if dag_run.conf['full_part_time_grp_uri'] else [],
            "serviceCenterSchedule": [
                {
                    "serviceCenter": {
                    "uri": dag_run.conf['standard_hours_grp_uri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
                ] if dag_run.conf['standard_hours_grp_uri'] else [],
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "uri": dag_run.conf['department_grp_uri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['department_grp_uri'] else [],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": dag_run.conf['employee_type_grp_uri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['employee_type_grp_uri'] else [],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": dag_run.conf['timesheet_period']
                    },
                    "effectiveDate": null
                }
            ],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrule_uri'] if dag_run.conf['payrule_uri'] else null,
                        "name": null if dag_run.conf['payrule_uri'] else '*Incyte_Placeholder Payrule'
                    },
                    "effectiveDate": null
                }
            ],
            "displayNameParameter": null,
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": []
        }
    }

def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

def put_timeoff_assignment_for_user(dag_run):
    timeofftype_uris = list(map(lambda x: x['timeoff_type_uri'] , rail.result('get_enabled_time_off_types')))
    return {
        "userUri": dag_run.conf['useruri'],
        "timeOffTypeUris": timeofftype_uris
    }

def get_default_timeoff_policy_schedule_payload(dag_run,for_each):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result(for_each)['timeoff_type_uri']
        }
    }

def get_user_timeoff_policy_payload(dag_run):
    return{
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('for_each_time_off_assign_default_policy')['timeoff_type_uri']
        },
        "policySetScheduleEntries": json.loads(rail.result('get_default_time_off_policy_schedule'))
    }

def validate_supervisor_changed():
    if not rail.result('get_effective_supervisor_of_user'):
        return True
    if rail.result('search_supervisor_in_replicon') and rail.result('get_effective_supervisor_of_user') and \
        rail.result('search_supervisor_in_replicon')['login_name'] == rail.result('get_effective_supervisor_of_user')['supervisor']['user']['loginName']:
        return False
    return True

def get_supervisor_message(status, action, details):
    # pylint: disable=too-many-return-statements
    if status == 'Error':
        return details
    if status == 'Exception' and not get_task_state('log_supervisor_not_present') == 'success' \
        and not get_task_state('log_supervisor_disabled_in_replicon') == 'success'  and details:
        return details
    if get_task_state('log_supervisor_not_present') == 'success':
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + ',Supervisor not present in replicon'+\
        (','+ details if status == 'Exception' and details else '')
    if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + ',Supervisor is disabled in replicon'+\
        (','+ details if status == 'Exception' and details else '')
    return f"""User {('Added' if action=='Add' else 'Updated')} Successfully"""

def get_supervisor_status(status, details):
    if status == 'Error':
        return 'Error'
    if get_task_state('log_supervisor_not_present') == 'success' or get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return 'Exception'
    if status == 'Exception' and not get_task_state('log_supervisor_not_present') == 'success' \
        and not get_task_state('log_supervisor_disabled_in_replicon') == 'success' and details:
        return status
    return 'Success'

def validate_enddate(dag_run):
    return datetime.strptime(dag_run.conf['end_date'], DATE_FORMAT) > datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT)

def validate_rehire(dag_run):
    return not rail.result('get_user_info')['userDetails']['isEnabled'] and not dag_run.conf['end_date']

def is_country_changed(dag_run):
    return rail.result('get_effective_user_groupmembership','location').get('uri', '') != dag_run.conf['country_grp_uri']

def update_holiday_calendar(dag_run):
    if not is_country_changed(dag_run) and not validate_rehire(dag_run) :
        return null
    if not dag_run.conf['holiday_calender_name']:
        return null
    current_holiday_calendar = rail.result("get_user_info")['holidayCalendar']
    if current_holiday_calendar and current_holiday_calendar['displayText'] == dag_run.conf['holiday_calender_name']:
        return null
    return  {
        "holidayCalendar": {
            "uri": dag_run.conf['holiday_calender_uri'],
            "name": null
        } if dag_run.conf['holiday_calender_uri'] else null
    }

def get_effective_date(dag_run):
    if validate_rehire(dag_run):
        return get_replicon_date(dag_run.conf['start_date'])
    if is_country_changed(dag_run):
        return get_replicon_date(dag_run.conf['todays_date'])
    return null

def update_country_grp(country_uri, current_country_uri, dag_run):
    return {
        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementLocationSchedule": [],
        "updateLocationScheduleOverDateRange": {
            "replacementLocationScheduleEntries": [
                {
                    "location": {
                        "uri": country_uri
                    },
                    "effectiveDate": get_effective_date(dag_run) if current_country_uri else null
                }
            ],
            "endDate": null
        }
    } if current_country_uri != country_uri else null

def update_department_grp(department_uri, current_departmen_uri, dag_run):
    return {
        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementDepartmentGroupSchedule": [],
        "updateDepartmentGroupScheduleOverDateRange": {
            "replacementDepartmentGroupScheduleEntries": [
                {
                    "departmentGroup": {
                        "uri": department_uri
                    },
                    "effectiveDate": get_effective_date(dag_run) if current_departmen_uri else null
                }
            ],
            "endDate": null
        }
    } if department_uri != current_departmen_uri else null

def update_employee_type_grp(employee_type_uri, current_employee_type_uri, dag_run):
    return {
        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementEmployeeTypeGroupSchedule": [],
        "updateEmployeeTypeGroupScheduleOverDateRange": {
            "replacementEmployeeTypeGroupScheduleEntries": [
                {
                    "employeeTypeGroup": {
                        "uri": employee_type_uri
                    },
                    "effectiveDate": get_effective_date(dag_run) if current_employee_type_uri else null
                }
            ],
            "endDate": null
        }
    } if employee_type_uri != current_employee_type_uri else null

def update_full_part_time_grp(full_part_time_grp_uri, current_full_part_time_grp_uri, dag_run):
    return {
        "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementCostCenterSchedule": [],
        "updateCostCenterScheduleOverDateRange": {
            "replacementCostCenterScheduleEntries": [
                {
                    "costCenter": {
                        "uri": full_part_time_grp_uri
                    },
                    "effectiveDate": get_effective_date(dag_run) if current_full_part_time_grp_uri else null
                }
            ],
            "endDate": null
        }
    } if full_part_time_grp_uri != current_full_part_time_grp_uri else null

def update_standard_hours_grp(standard_hours_grp_uri, current_standard_hours_grp_uri, dag_run):
    return {
      "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
      "replacementServiceCenterSchedule": [],
      "updateServiceCenterScheduleOverDateRange": {
        "replacementServiceCenterScheduleEntries": [
          {
            "serviceCenter": {
              "uri": standard_hours_grp_uri,
              "parentUri": null,
              "name": null
            },
            "effectiveDate": get_effective_date(dag_run) if current_standard_hours_grp_uri else null
          }
        ],
        "endDate": null
      }
    } if standard_hours_grp_uri != current_standard_hours_grp_uri else null

def update_work_location_grp(work_location_grp_uri, current_work_location_grp_uri, dag_run):
    return {
        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementDivisionSchedule": [],
        "updateDivisionScheduleOverDateRange": {
            "replacementDivisionScheduleEntries": [
                {
                    "division": {
                        "uri": work_location_grp_uri
                    },
                    "effectiveDate": get_effective_date(dag_run) if current_work_location_grp_uri else null
                }
            ],
            "endDate": null
        }
    } if work_location_grp_uri != current_work_location_grp_uri else null

def update_timesheet_period(dag_run):
    if not validate_rehire(dag_run):
        return null

    current_timesheet_period = rail.result("get_user_info")['timesheetPeriodSchedule']

    return {
        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementTimesheetPeriodSchedule": [],
        "updateTimesheetPeriodScheduleOverDateRange": {
            "replacementTimesheetPeriodScheduleEntries": [
                {
                    "timesheetPeriod": {
                        "name": dag_run.conf['timesheet_period'],
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['start_date']) if not current_timesheet_period else null
                }
            ],
            "endDate": null
        }
    } if current_timesheet_period != dag_run.conf['timesheet_period'] else null

def update_timesheet_approval_path(dag_run):
    if validate_rehire(dag_run):
        return {
            "uri": dag_run.conf['timesheet_approval_path_uri'] if dag_run.conf['timesheet_approval_path_uri'] else null,
            "name": null if dag_run.conf['timesheet_approval_path_uri'] else 'System Approval'
            }
    if not is_country_changed(dag_run):
        return null

    current_timesheet_approval_path = rail.result("get_user_info")['timesheetApprovalPath']

    if not current_timesheet_approval_path and dag_run.conf['timesheet_approval_path_uri']:
        return {
            "uri": dag_run.conf['timesheet_approval_path_uri'] if dag_run.conf['timesheet_approval_path_uri'] else null,
            "name": null if dag_run.conf['timesheet_approval_path_uri'] else 'System Approval'
            }
    if dag_run.conf['timesheet_approval_path_uri'] and dag_run.conf['timesheet_approval_path_uri']!= \
        current_timesheet_approval_path['uri']:
        return {
            "uri": dag_run.conf['timesheet_approval_path_uri'] if dag_run.conf['timesheet_approval_path_uri'] else null,
            "name": null if dag_run.conf['timesheet_approval_path_uri'] else 'System Approval'
            }
    return null

def update_policy_set(dag_run):
    if not validate_rehire(dag_run) and not is_country_changed(dag_run):
        return null
    policy_set_to_assign = []
    policy_set_to_remove = []

    assigned_timesheet_template = rail.result("get_user_info")['timesheetTemplate']
    assigned_timeoff_template = rail.result("get_user_info")['timeOffTemplate']
    if (not assigned_timesheet_template and dag_run.conf['timesheet_template_uri']) or \
        ((assigned_timesheet_template and dag_run.conf['timesheet_template_uri']) \
        and (dag_run.conf['timesheet_template_uri'] != assigned_timesheet_template['uri'])):
        policy_set_to_assign.append(dag_run.conf['timesheet_template_uri'])

    if (not assigned_timeoff_template and dag_run.conf['timeoff_template_uri']) or \
        ((assigned_timeoff_template and dag_run.conf['timeoff_template_uri']) \
        and (dag_run.conf['timeoff_template_uri'] != assigned_timeoff_template['uri'])):
        policy_set_to_assign.append(dag_run.conf['timeoff_template_uri'])

    if not dag_run.conf['timeoff_template_uri']:
        if assigned_timeoff_template:
            policy_set_to_remove.append(assigned_timeoff_template['uri'])

    if policy_set_to_assign or policy_set_to_remove:
        return {
            "policySetUrisToAssign": policy_set_to_assign,
            "policyUrisToRemovePolicySet": policy_set_to_remove
        }
    return null

def update_security_settings(dag_run):
    if validate_rehire(dag_run):
        if rail.result("get_user_info")['securityConfiguration']['enabledAuthenticationTypeUris'][0] != 'urn:replicon:user-authentication-type:sso':
            return {
                "loginName": null,
                "ssoName": dag_run.conf['login_name'],
                "enabledAuthenticationTypeUris": ["urn:replicon:user-authentication-type:sso"],
                "emailMFAResendVerificationEmail": "false",
                "emailMFATryAddMethodFromUsersEmail": "false",
                "clearIsLockedOut": "false"
                }
    return null

def update_user_details(dag_run):
    user_details = rail.result("get_user_info")['userDetails']

    return {
      "firstName": dag_run.conf['first_name'] if user_details['firstName'] != dag_run.conf['first_name'] else null,
      "lastName": dag_run.conf['last_name'] if user_details['lastName'] != dag_run.conf['last_name'] else null,
      "emailAddress": {
        "emailAddress": dag_run.conf['email_id']
      } if dag_run.conf['email_id'] and user_details['emailAddress'] != dag_run.conf['email_id'] else null,
      "language": null,
      "employmentDateRange": null,
      "employmentStartDate": {
        "date": get_replicon_date(dag_run.conf['start_date'])
      } if user_details['employmentDateRange']['startDate'] != get_replicon_date(dag_run.conf['start_date']) else null,
       "employmentEndDate": {
         "date": null
       },
    }
def get_effective_date_for_payrule(dag_run):
    if validate_rehire(dag_run):
        return get_replicon_date(datetime.now().replace(day=1).strftime(DATE_FORMAT))
    if is_country_changed(dag_run):
        return get_replicon_date((datetime.now().replace(day=1)+timedelta(days=32)).replace(day=1).strftime(DATE_FORMAT))
    return null

def update_payrule_script(dag_run):
    current_payrulescript = rail.result("get_user_info")['payRuleScriptSchedule']
    if not current_payrulescript:
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrule_uri'] if dag_run.conf['payrule_uri'] else null,
                        "name": null if dag_run.conf['payrule_uri'] else '*Incyte_Placeholder Payrule'
                    },
                    "effectiveDate": get_effective_date_for_payrule(dag_run)
                }
            ]
        }

    if not validate_rehire(dag_run) and not is_country_changed(dag_run):
        return null

    if dag_run.conf['payrule_name'] != current_payrulescript[-1]['payRuleScript']['displayText']:
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrule_uri'] if dag_run.conf['payrule_uri'] else null,
                        "name": null if dag_run.conf['payrule_uri'] else '*Incyte_Placeholder Payrule'
                    },
                    "effectiveDate": get_effective_date_for_payrule(dag_run)
                }
            ]
        }

    return null

def update_schedule(dag_run):
    current_schedule = rail.result("get_user_info")['schedulePolicies']

    if not validate_rehire(dag_run) and not is_country_changed(dag_run):
        return null

    if not current_schedule:
        return {
            "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementSchedule": [],
            "updateScheduleOverDateRange": {
                "replacementScheduleEntries": [
                {
                    "schedulePolicy": {
                    "officeScheduleUri": dag_run.conf['schedule_uri'] if dag_run.conf['schedule_uri'] else null,
                    "name": null if dag_run.conf['schedule_uri'] else "8 hours/day; Mon-Fri",
                    "officeSchedule": {
                        "officeScheduleUri": dag_run.conf['schedule_uri'] if dag_run.conf['schedule_uri'] else null,
                        "name": null if dag_run.conf['schedule_uri'] else "8 hours/day; Mon-Fri"
                    },
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": get_effective_date(dag_run)
                }
                ],
                "endDate": null
            }
            }

    if dag_run.conf['schedule_name'] != (current_schedule[-1]['officeSchedule']['displayText'] if current_schedule[-1]['scheduleTypeUri']==
        'urn:replicon:schedule-type:office-schedule' else null):
        return {
            "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementSchedule": [],
            "updateScheduleOverDateRange": {
                "replacementScheduleEntries": [
                {
                    "schedulePolicy": {
                    "officeScheduleUri": dag_run.conf['schedule_uri'] if dag_run.conf['schedule_uri'] else null,
                    "name": null if dag_run.conf['schedule_uri'] else "8 hours/day; Mon-Fri",
                    "officeSchedule": {
                        "officeScheduleUri": dag_run.conf['schedule_uri'] if dag_run.conf['schedule_uri'] else null,
                        "name": null if dag_run.conf['schedule_uri'] else "8 hours/day; Mon-Fri"
                    },
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": get_effective_date(dag_run)
                }
                ],
                "endDate": null
            }
            }

    return null

def update_notification_preferences(dag_run):
    if validate_rehire(dag_run):
        return {
            "notificationDeliveryPreferencesToApply": [
            {
                "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
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
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:holiday",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
            }
            ],
            "sharedDeliveryPreferenceOptionUris": [
            "urn:replicon:user-shared-delivery-preference-option:do-not-deliver-on-non-work-days"
            ]
        }
    return null

def apply_user_modifications_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "holidayCalendarToApply": update_holiday_calendar(dag_run),
            "schedulePolicyToApply": update_schedule(dag_run),
            "locationScheduleToApply": update_country_grp(dag_run.conf['country_grp_uri'],
                rail.result('get_effective_user_groupmembership','location').get('uri', ''), dag_run),
            "divisionScheduleToApply": update_work_location_grp(dag_run.conf['work_location_grp_uri'],
                rail.result('get_effective_user_groupmembership', 'division').get('uri', ''), dag_run)  if dag_run.conf['work_location_name'] else null,
            "costCenterScheduleToApply": update_full_part_time_grp(dag_run.conf['full_part_time_grp_uri'],
                rail.result('get_effective_user_groupmembership', 'costcenter').get('uri', ''), dag_run)  if dag_run.conf['full_part_time'] else null,
            "departmentGroupScheduleToApply": update_department_grp(dag_run.conf['department_grp_uri'],
                rail.result('get_effective_user_groupmembership', 'department').get('uri', ''), dag_run)  if dag_run.conf['dept_full_path'] else null,
            "employeeTypeGroupScheduleToApply": update_employee_type_grp(dag_run.conf['employee_type_grp_uri'],
                rail.result('get_effective_user_groupmembership', 'employeetype').get('uri', ''), dag_run)  if dag_run.conf['employee_type'] else null,
            "serviceCenterScheduleToApply": update_standard_hours_grp(dag_run.conf['standard_hours_grp_uri'],
                rail.result('get_effective_user_groupmembership', 'servicecenter').get('uri', ''), dag_run) if dag_run.conf['standard_hours'] else null,
            "timesheetPeriodScheduleToApply": update_timesheet_period(dag_run),
            "timesheetApprovalPathToApply": update_timesheet_approval_path(dag_run),
            "policySetsToApply": update_policy_set(dag_run),
            "notificationPreferencesToApply": update_notification_preferences(dag_run),
            "securitySettingsToApply": update_security_settings(dag_run),
            "customFieldValuesToApply": get_udfs('updateuser', dag_run),
            "userDetailsToApply": update_user_details(dag_run),
            "payRulesScheduleModifications": update_payrule_script(dag_run),
            },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_permission_set_rehire_payload(dag_run):
    permissions_uris_to_assign = list(map (lambda y:y['uri'], (filter(lambda x : x['displayText'] in ['Basic User with Reports', 'Supervisor']
            and x['uri'],rail.result('get_user_info')['permissionSets']))))
    if not permissions_uris_to_assign:
        permissions_uris_to_assign.append(dag_run.conf['basic_user_permission_uri'])
    return {
        "userUri": dag_run.conf['useruri'],
        "permissionSetUris": permissions_uris_to_assign
    }

def get_update_user_message():
    # pylint: disable=too-many-return-statements
    if rail.result('log_supervisor_not_present'):
        return ""
    if get_task_state('log_supervisor_disabled_in_replicon') =='success':
        return 'User Partially Updated, Supervisor is disabled in replicon'
    return "User Updated"

def get_update_user_severity():
    if  rail.result('log_supervisor_not_present') or get_task_state('log_supervisor_disabled_in_replicon')=='success':
        return 'Exception'
    return 'Success'

def get_add_user_message():
    # pylint: disable=too-many-return-statements
    if rail.result('log_supervisor_not_present'):
        return ""
    if get_task_state('log_supervisor_disabled_in_replicon') =='success':
        return 'User Partially Added, Supervisor is disabled in replicon'
    return "User Added Successfully"

def get_add_user_severity():
    if rail.result('log_supervisor_not_present') or get_task_state('log_supervisor_disabled_in_replicon') =='success':
        return 'Exception'
    return 'Success'

def put_user_notification_preferences_payload():
    return {
        "user": {
            "uri": rail.result('add_new_user')['uri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "preferences": {
            "notificationDeliveryPreferences": [
            {
                "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
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
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:holiday",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
            }
            ],
            "sharedDeliveryPreferenceOptionUris": [
                "urn:replicon:user-shared-delivery-preference-option:do-not-deliver-on-non-work-days"
            ]
        }
        }

def get_custom_timeoff_policy_schedule_payload(dag_run,config):
    data = rail.result('get_all_time_off_types')
    def get_time_off_type_uri():
        if dag_run.conf['country_name']=="Austria":
            return rail.find_first_by_attr_and_get_attr(data,'timeoff_type_name',config.REFERENCE_TIME_OFF_AUSTRIA, 'timeoff_type_uri')
        if dag_run.conf['country_name']=="Germany":
            return rail.find_first_by_attr_and_get_attr(data,'timeoff_type_name',config.REFERENCE_TIME_OFF_GERMANY, 'timeoff_type_uri')
        return rail.find_first_by_attr_and_get_attr(data,'timeoff_type_name',"Compensation Day", 'timeoff_type_uri')

    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": get_time_off_type_uri()
        }
    }

def put_user_timeoff_policy_rehire(dag_run):
    def get_policy_set():
        if rail.result('for_each_time_off_type_policy')['timeoff_type_name'] != "Compensation Day" and rail.result('get_default_time_off_policy_schedule'):
            return ast.literal_eval(str(rail.result('get_default_time_off_policy_schedule')).replace("'script'", "'scriptTarget'"))
        if rail.result('for_each_time_off_type_policy')['timeoff_type_name'] == "Compensation Day":
            return json.loads(rail.result('get_all_policy_to_assign_compensation_day_rehire'))
        return []
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('for_each_time_off_type_policy')['timeoff_type_uri']
        },
        "policySetScheduleEntries": get_policy_set()
    }

def put_timeoff_assignment_for_user_update(dag_run):
    timeofftype_uris = list(map(lambda x: x['timeoff_type_uri'] , rail.result('assigned_time_offs_types')))
    compensation_day_timeoff_uri = rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_off_types'),
        'timeoff_type_name',"Compensation Day", 'timeoff_type_uri')
    all_timeoff_type_uris = timeofftype_uris.append(compensation_day_timeoff_uri)
    return {
        "userUri": dag_run.conf['useruri'],
        "timeOffTypeUris": all_timeoff_type_uris
    }

def get_update_user_timeoff_policy_payload(dag_run):
    compensation_day_timeoff_uri = rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_off_types'),
        'timeoff_type_name',"Compensation Day", 'timeoff_type_uri')
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": compensation_day_timeoff_uri
        },
        "policySetScheduleEntries": json.loads(rail.result('get_all_policy_to_assign_compensation_day_update'))
    }

def get_timeoff_policy_schedule_update_payload(dag_run,config):
    data = rail.result('get_all_time_off_types')
    def get_time_off_type_uri():
        if dag_run.conf['country_name']=="Austria":
            return rail.find_first_by_attr_and_get_attr(data,'timeoff_type_name',config.REFERENCE_TIME_OFF_AUSTRIA, 'timeoff_type_uri')
        if dag_run.conf['country_name']=="Germany":
            return rail.find_first_by_attr_and_get_attr(data,'timeoff_type_name',config.REFERENCE_TIME_OFF_GERMANY, 'timeoff_type_uri')
        return rail.find_first_by_attr_and_get_attr(data,'timeoff_type_name',"Compensation Day", 'timeoff_type_uri')

    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": get_time_off_type_uri()
        }
    }
