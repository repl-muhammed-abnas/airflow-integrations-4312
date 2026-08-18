from datetime import datetime
from os import path
import json
from uuid import uuid4
import rail
from rail.filters import split

null = None
DATE_FORMAT = "%m/%d/%Y"

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

def get_mapper_location_codes(mapper):
    return tuple(list(set(map(lambda item:item['location_code'],mapper))))

MANDATORY_FIELDS = {
        "employee_id":"EMPLOYEE_NUMBER",
        "person_type": "PERSON_TYPE",
        "first_name": "FIRST_NAME",
        "last_name": "LAST_NAME",
        "feed_file_start_date": "HIRE_DATE",
        "supervisor_id": "MANAGER_EMPLOYEE_NUMBER",
        "location_code": "LOCATION_CODE",
        "location_name": "LOCATION_NAME",
        "expected_weekly_hours": "EXPECTED_WEEKLY_HOURS",
        "department_name": "DEPARTMENT",
        "employee_status": "EMPLOYEE_STATUS",
        "email": "EMAIL_ADDRESS",
        "employee_type_level_1": "EMPLOYEE_TYPE",
        "employee_type_level_2": "Assignment_Category_1",
        "employee_type_level_3": "Assignment_Category_2",
        "feed_file_effective_date": "LAST_UPDATE_DATE",
        "tax_id": "TAX_ID",
}

def get_mandatory_fields_exception_message(item, mapper):
    missing_fields = []
    if item['location_code'] not in get_mapper_location_codes(mapper):
        missing_fields.append(f"Location Code-{item['location_code']} is not available in Mapper")
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")

def get_location_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
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

def get_process_users_conf(item, mapper, punch_policy_name, payrule_mapper):
    get_user_udfs = rail.result('get_user_udfs')
    get_user_sync_mapper = rail.find_first_by_attr_and_get_attr(mapper, 'location_code', item['location_code'])

    def get_payrule_name():

        _data = list(filter(lambda row: row['location_code']==item['location_code']
                           and row['employee_type_1'] == item['employee_type_level_1']
                           and row['employee_type_2'] == item['employee_type_level_2']
                           and row['employee_type_3'] == item['employee_type_level_3']
                           and row['expected_weekly_hours'] == item['expected_weekly_hours'], payrule_mapper))
        if _data:
            return _data[0]['payrule']
        return null

    def get_employee_type_full_path():
        return item['employee_type_level_1']+"|"+ item['employee_type_level_2']+"|"+ item['employee_type_level_3']

    def get_timesheet_template_name():
        if item['person_type'] == 'P':
            return 'Clock In and Out'
        if item['person_type'] == 'M':
            return 'Clock In and Out with Allocation'
        return null

    return {
        **{
            'file_name': split(string=path.split(rail.result("new_file_sensor"))[1], separator=".")[0]+'.csv',
        },
        **item,
        **{
            'start_date': item['feed_file_start_date'].split(' ')[0],
            'end_date': item['feed_file_end_date'].split(' ')[0] if item['feed_file_end_date'] else null,
            'effective_date': item['feed_file_effective_date'].split(' ')[0] if item['feed_file_effective_date'] else null,
            'person_type_definitionuri': get_user_udfs['person_type_definitionuri'],
            'job_title_definitionuri': get_user_udfs['job_title_definitionuri'],
            'job_code_definitionuri': get_user_udfs['job_code_definitionuri'],
            'tax_id_definitionuri': get_user_udfs['tax_id_definitionuri'],
            'expected_weekly_hrs_definitionuri': get_user_udfs['expected_weekly_hrs_definitionuri'],
            'person_type_dropdownuri': rail.find_first_by_attr_and_get_attr
                (rail.result("get_person_type_udf_dropdown_values"),'name', item['person_type'],'uri')
                if item['person_type'] else null,

            'department_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_departments'), 'name', item['department_name'], 'uri'),
            'employee_type_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_all_employee_types'), 'full_path', get_employee_type_full_path(), 'uri'),
            'location_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_all_locations'), 'code', item['location_code'], 'uri'),

            'user_permissionset_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'),'displayText',"Basic User with Reports",'uri'),
            'manager_permissionset_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'),'displayText','Supervisor','uri'),

            'timesheet_template_name': get_timesheet_template_name(),
            'timesheet_template_uri': rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_policy_sets"),'displayText',get_timesheet_template_name(),"uri")
                if get_timesheet_template_name() else null,
            'timesheet_approvalpath_name': get_user_sync_mapper['timesheet_approval_path'],
            'timesheet_approvalpath_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_timesheet_approval_paths"),'displayText',
                get_user_sync_mapper['timesheet_approval_path'],"uri") if get_user_sync_mapper['timesheet_approval_path'] else null,
            'timesheet_period': get_user_sync_mapper['timesheet_period'],

            'timeoff_template_name': get_user_sync_mapper['timeoff_template'],
            'timeoff_template_uri': rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_policy_sets"),'displayText',get_user_sync_mapper['timeoff_template'],"uri")
                if get_user_sync_mapper['timeoff_template'] else null,
            'timeoff_approvalpath_name':get_user_sync_mapper['timeoff_approval_path'],
            'timeoff_approvalpath_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_timeoff_approval_paths"),'displayText',
                get_user_sync_mapper['timeoff_approval_path'],"uri") if get_user_sync_mapper['timeoff_approval_path'] else null,

            'work_week_uri': get_user_sync_mapper['work_week_uri'],
            'holiday_calender': get_user_sync_mapper['holiday_calender'],

            'timezone':get_user_sync_mapper['timezone'],
            'timezone_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_timezones'), 'displayText', get_user_sync_mapper['timezone'], 'uri')
                if get_user_sync_mapper['timezone'] else null,

            'payrule_name': get_payrule_name(),
            'payrule_script_uri': rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_payrule_scripts"),'displayText', get_payrule_name(),"uri")
                if get_payrule_name() else null,
            'supervisor_log' : rail.result('create_supervisor_log'),
            'get_required_time_off_types_names': get_user_sync_mapper['time_off_types'],

            'punch_policy_name': punch_policy_name,
            'punch_policy_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_sets"),'displayText',punch_policy_name,"uri")
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
            'todaysdate': (datetime.now()).strftime(DATE_FORMAT)
        }
    }

def get_add_employeetype_payload(dag_run):
    return {
        "employeeTypeGroup": {
            "parent": {
                "uri": rail.result("get_parent_employee_type_details")[0]['uri']
            },
        } if rail.result('get_parent_employee_type_details') else null,
        "modifications": {
            "name": dag_run.conf['employeetype_name'],
            "isEnabled": "1"
        },
        "unitOfWorkId": str(uuid4())
    }


def get_user_data_payload(dag_run):
    return{
    "users": [
        {
        "uri": null,
        "loginName": null,
        "employeeId": dag_run.conf['employee_id'],
        "parameterCorrelationId": null
        }
    ]
}

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

def test_valid_fields(dag_run):
    startdate = get_replicon_date(dag_run.conf['start_date'])
    if not startdate:
        return False
    if dag_run.conf['end_date']:
        enddate = get_replicon_date(dag_run.conf['end_date'])
        if not enddate:
            return False
    if dag_run.conf['employee_status'] == "Terminated" and not dag_run.conf['end_date']:
        return False
    return True

def get_invalid_fields_message(dag_run):
    log=[]
    startdate = get_replicon_date(dag_run.conf['start_date'])
    if not startdate:
        log.append('Invalid format for Hire Date')
    if dag_run.conf['end_date']:
        enddate = get_replicon_date(dag_run.conf['end_date'])
        if not enddate:
            log.append('Invalid format for Termination Date')
    if dag_run.conf['employee_status'] == "Terminated" and not dag_run.conf['end_date']:
        log.append('Employee Status field is Terminated in Feed File but Termination date is blank')
    if dag_run.conf['employee_status'] == "Active" and dag_run.conf['end_date']:
        log.append('Employee Status field is Active in Feed File but Termination date is present')
    return rail.smartjoin_by_delim(log,";")

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
        add_udf_field_values(definitionuri = dag_run.conf['person_type_definitionuri'], dropdownuri= dag_run.conf['person_type_dropdownuri'])
        if dag_run.conf['job_title']:
            add_udf_field_values(definitionuri = dag_run.conf['job_title_definitionuri'], textvalue = dag_run.conf['job_title'])
        if dag_run.conf['job_code']:
            add_udf_field_values(definitionuri = dag_run.conf['job_code_definitionuri'],textvalue = dag_run.conf['job_code'])
        if dag_run.conf['tax_id']:
            add_udf_field_values(definitionuri = dag_run.conf['tax_id_definitionuri'],textvalue = dag_run.conf['tax_id'])
        if dag_run.conf['expected_weekly_hours']:
            add_udf_field_values(definitionuri = dag_run.conf['expected_weekly_hrs_definitionuri'],number = dag_run.conf['expected_weekly_hours'])

    if userstatus =='updateuser':
        current_person_type = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'PERSON_TYPE', 'text')
        if current_person_type != dag_run.conf['person_type']:
            add_udf_field_values(definitionuri = dag_run.conf['person_type_definitionuri'], dropdownuri= dag_run.conf['person_type_dropdownuri'])

        current_job_title = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Job Title', 'text')
        if current_job_title != dag_run.conf['job_title']:
            add_udf_field_values(definitionuri = dag_run.conf['job_title_definitionuri'], textvalue= dag_run.conf['job_title'])

        current_job_code = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Job Code', 'text')
        if current_job_code != dag_run.conf['job_code']:
            add_udf_field_values(definitionuri = dag_run.conf['job_code_definitionuri'], textvalue= dag_run.conf['job_code'])

        current_weekly_hrs = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Expected Weekly Hours', 'text')
        if current_weekly_hrs != dag_run.conf['expected_weekly_hours']:
            add_udf_field_values(definitionuri = dag_run.conf['expected_weekly_hrs_definitionuri'], number= dag_run.conf['expected_weekly_hours'])

    return udfs

def get_put_user_payload(dag_run):
    log=[]
    put_user_payload = {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['email'],
            },
            "firstname": dag_run.conf['first_name'],
            "lastname": dag_run.conf['last_name'],
            "emailAddress": dag_run.conf['email'],
            "employeeId": dag_run.conf['employee_id'],
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": null,
                        "officeSchedule": null,
                        "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                    },
                    "effectiveDate": null
                }
            ],
            "workWeekStartDayUri": dag_run.conf['work_week_uri'],
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
                "isLoginEnabled": "true" if dag_run.conf['employee_status'] == "Active" else "false",
                "loginName": dag_run.conf['email'],
                "SSOName": dag_run.conf['email'],
            },
            "holidayCalendar": {
                "uri": null,
                "name": dag_run.conf['holiday_calender']
            },
            "permissionSets": [
                {
                    "uri": dag_run.conf['user_permissionset_uri'],
                    "name": null
                }
            ] if dag_run.conf['person_type'] =="P" else [
                {
                    "uri": dag_run.conf['user_permissionset_uri'],
                    "name": null
                },
                {
                    "uri": dag_run.conf['manager_permissionset_uri'],
                    "name": null
                }
            ],
            "policySets": [{
                    "uri": dag_run.conf['timesheet_template_uri'],
                    "name": null
                },
                {
                    "uri": dag_run.conf['timeoff_template_uri'],
                    "name": null
                },
                {
                    "uri": dag_run.conf['punch_policy_uri'],
                    "name": null
                }],
            "timesheetApprovalPath": {
                "uri": dag_run.conf['timesheet_approvalpath_uri'],
                "name": null
                },
            "timeOffApprovalPath":  {
                "uri": dag_run.conf['timeoff_approvalpath_uri'],
                "name": null
                },
            "customFieldValues": get_udfs('adduser', dag_run),
            "assignedActivities": [],
            "timeZone":{
                "uri": dag_run.conf['timezone_uri'],
                "IANAName": null
            },
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [
                {
                    "location": {
                        "uri": dag_run.conf['location_uri'],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ],
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "uri": dag_run.conf['department_uri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": dag_run.conf['employee_type_uri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ],
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
                        "uri": dag_run.conf['payrule_script_uri'],
                        "name": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['payrule_script_uri'] else [],
            "displayNameParameter": null,
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": []
        }
    }

    if not dag_run.conf['payrule_name']:
        log.append("Payrule not added as Payrule not found for received combination in mapper")

    if dag_run.conf['payrule_name'] and not dag_run.conf['payrule_script_uri']:
        log.append("Payrule not added as payrule not found / disabled in Replicon Instance.")

    rail.set_result(key="exception_logs",val= log)

    return put_user_payload

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

def validate_supervisor_changed():
    if not rail.result('get_effective_supervisor_of_user'):
        return True
    if rail.result('search_supervisor_in_replicon') and rail.result('get_effective_supervisor_of_user') and \
        rail.result('search_supervisor_in_replicon')['uri'] == rail.result('get_effective_supervisor_of_user')['supervisor']['user']['uri']:
        return False
    return True


def get_add_user_message():
    # pylint: disable=too-many-return-statements
    exception_logs = rail.result("add_new_user", "exception_logs")
    if get_task_state('log_supervisor_not_present') == 'success':
        return ""
    if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        exception_logs.append('Supervisor is disabled in replicon')
    if exception_logs:
        return f"User Partially Added; {rail.smartjoin_by_delim(exception_logs, ';')}"
    return "User Added Successfully"

def get_add_user_severity():
    if rail.result("add_new_user", "exception_logs"):
        return 'Exception'
    if get_task_state('log_supervisor_not_present') == 'success'\
        or get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return 'Exception'
    return 'Success'

def validate_enddate(dag_run):
    return datetime.strptime(dag_run.conf['end_date'], DATE_FORMAT) > datetime.strptime(dag_run.conf['start_date'], DATE_FORMAT)

def get_filtered_time_off_types(response):
    return list(map(lambda item: {
        "timeoff_type_name": item['displayText'],
        'timeoff_type_uri': item['uri'],
    }, response))

def put_timeoff_assignment_for_user(dag_run):
    timeofftype_uris = list(map(lambda x: x['timeoff_type_uri'] , rail.result('get_required_time_off_type_details_to_assign')['result']))
    return {
        "userUri": dag_run.conf['useruri'],
        "timeOffTypeUris": timeofftype_uris
    }

def get_default_timeoff_policy_schedule_payload(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('for_each_time_off_assign_default_policy')['timeoff_type_uri']
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

def validate_rehire(dag_run):
    return not rail.result('get_user_info')['userDetails']['isEnabled'] and dag_run.conf['employee_status'] == 'Active'


def update_user_details(dag_run):
    user_details = rail.result("get_user_info")['userDetails']

    def update_first_name(dag_run):
        if user_details['firstName']!= dag_run.conf['first_name']:
            return dag_run.conf['first_name']
        return null

    return {
      "firstName": update_first_name(dag_run),
      "lastName": dag_run.conf['last_name'] if user_details['lastName'] != dag_run.conf['last_name'] else null,
      "emailAddress": {
        "emailAddress": dag_run.conf['email']
      } if user_details['emailAddress'] != dag_run.conf['email'] else null,
      "language": null,
      "employmentDateRange": null,
      "employmentStartDate": {
        "date": get_replicon_date(dag_run.conf['start_date'])
      } if user_details['employmentDateRange']['startDate'] != get_replicon_date(dag_run.conf['start_date']) else null,
       "employmentEndDate": {
         "date": null
       },
    }

def update_payrule_script(dag_run):
    if not dag_run.conf['payrule_script_uri']:
        return null
    current_payrulescript = rail.result("get_user_info")['payRuleScriptSchedule']
    if not current_payrulescript:
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrule_script_uri'],
                        "name": null
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effective_date'])
                }
            ]
        }

    if dag_run.conf['payrule_name'] != current_payrulescript[-1]['payRuleScript']['displayText']:
        return {
            "scheduleEntries": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrule_script_uri'],
                        "name": null
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effective_date'])
                }
            ]
        }

    return null


def update_location_grp(locationuri, currentlocationuri, dag_run):
    return {
        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementLocationSchedule": [],
        "updateLocationScheduleOverDateRange": {
            "replacementLocationScheduleEntries": [
                {
                    "location": {
                        "uri": locationuri
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effective_date'])
                }
            ],
            "endDate": null
        }
    } if currentlocationuri != locationuri else null

def update_department_grp(departmenturi, currentdepartmenturi, dag_run):
    return {
        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementDepartmentGroupSchedule": [],
        "updateDepartmentGroupScheduleOverDateRange": {
            "replacementDepartmentGroupScheduleEntries": [
                {
                    "departmentGroup": {
                        "uri": departmenturi
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effective_date'])
                }
            ],
            "endDate": null
        }
    } if departmenturi != currentdepartmenturi else null


def update_employeetype_grp(employeetypeuri, currentemployeetypeuri, dag_run):
    return {
        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementEmployeeTypeGroupSchedule": [],
        "updateEmployeeTypeGroupScheduleOverDateRange": {
            "replacementEmployeeTypeGroupScheduleEntries": [
                {
                    "employeeTypeGroup": {
                        "uri": employeetypeuri
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effective_date'])
                }
            ],
            "endDate": null
        }
    } if employeetypeuri != currentemployeetypeuri else null

def update_holiday_calendar(dag_run):
    current_holiday_calendar = rail.result("get_user_info")['holidayCalendar']
    if current_holiday_calendar and current_holiday_calendar['displayText'] == dag_run.conf['holiday_calender']:
        return null
    return  {
        "holidayCalendar": {
            "uri": null,
            "name": dag_run.conf['holiday_calender']
        }
    }

def update_security_settings(dag_run):
    def is_email_changed():
        return bool(rail.result("get_user_info")['userDetails']['emailAddress'] != dag_run.conf['email'])

    if is_email_changed():
        return {
            "loginEnabled": "true",
            "loginName": dag_run.conf['email'],
            "ssoName": dag_run.conf['email'],
            "enabledAuthenticationTypeUris": ["urn:replicon:user-authentication-type:sso"],
            "emailMFAResendVerificationEmail": "false",
            "emailMFATryAddMethodFromUsersEmail": "false",
            "clearIsLockedOut": "false"
            }
    return null


def update_permission_set(dag_run):
    basic_user_permission = rail.find_first_by_attr_and_get_attr(rail.result('get_user_info')['permissionSets'],
            'displayText', 'Basic User with Reports', 'displayText')
    manager_permission = rail.find_first_by_attr_and_get_attr(rail.result('get_user_info')['permissionSets'],
            'displayText', 'Supervisor', 'displayText')
    if not basic_user_permission or not manager_permission:
        if dag_run.conf['person_type']=='P' and not basic_user_permission:
            return {
                "permissionSetUrisToAssign": [
                    dag_run.conf['user_permissionset_uri']
                ],
                "policyUrisToRemovePermissionSet": []
            }
        if dag_run.conf['person_type']=='M':
            if not basic_user_permission and manager_permission:
                return {
                    "permissionSetUrisToAssign": [
                        dag_run.conf['user_permissionset_uri']
                    ],
                    "policyUrisToRemovePermissionSet": []
                    }

            if basic_user_permission and not manager_permission:
                return {
                    "permissionSetUrisToAssign": [
                        dag_run.conf['manager_permissionset_uri']
                    ],
                    "policyUrisToRemovePermissionSet": []
                    }
            if not basic_user_permission and not manager_permission:
                return {
                    "permissionSetUrisToAssign": [
                        dag_run.conf['user_permissionset_uri'],
                        dag_run.conf['manager_permissionset_uri']
                    ],
                    "policyUrisToRemovePermissionSet": []
                    }
    return null

def update_policy_set(dag_run):
    policy_set_to_assign = []
    assigned_timesheet_template = rail.result("get_user_info")['timesheetTemplate']
    assigned_timeoff_template = rail.result("get_user_info")['timeOffTemplate']
    assigned_punch_policy = rail.result("get_assigned_policy_to_user")

    if not assigned_timesheet_template or (assigned_timesheet_template and dag_run.conf['timesheet_template_uri']) \
        and (dag_run.conf['timesheet_template_uri'] != assigned_timesheet_template['uri']):
        policy_set_to_assign.append(dag_run.conf['timesheet_template_uri'])

    if not assigned_timeoff_template or (assigned_timeoff_template and dag_run.conf['timeoff_template_uri']) \
        and (dag_run.conf['timeoff_template_uri'] != assigned_timeoff_template['uri']):
        policy_set_to_assign.append(dag_run.conf['timeoff_template_uri'])

    if dag_run.conf['punch_policy_name'] and (not assigned_punch_policy or (( assigned_punch_policy and dag_run.conf['punch_policy_uri']) \
        and (dag_run.conf['punch_policy_uri'] != assigned_punch_policy[0]['policySet']['uri']))):
        policy_set_to_assign.append(dag_run.conf['punch_policy_uri'])

    if policy_set_to_assign:
        return {
            "policySetUrisToAssign": policy_set_to_assign,
            "policyUrisToRemovePolicySet": []
        }
    return null

def update_timesheet_approval_path(dag_run):
    current_timesheet_approval_path = rail.result("get_user_info")['timesheetApprovalPath']
    if not current_timesheet_approval_path and dag_run.conf['timesheet_approvalpath_uri']:
        return {
            "uri": dag_run.conf['timesheet_approvalpath_uri'],
            "name": null
            }
    if dag_run.conf['timesheet_approvalpath_uri'] and dag_run.conf['timesheet_approvalpath_uri']!= \
        current_timesheet_approval_path['uri']:
        return {
            "uri": dag_run.conf['timesheet_approvalpath_uri'],
            "name": null
            }
    return null

def update_timeoff_approval_path(dag_run):
    current_timeoff_approval_path = rail.result("get_user_info")['timeOffApprovalPath']
    if not current_timeoff_approval_path and dag_run.conf['timeoff_approvalpath_uri']:
        return {
            "uri": dag_run.conf['timesheet_approvalpath_uri'],
            "name": null
            }
    if dag_run.conf['timeoff_approvalpath_uri'] and dag_run.conf['timeoff_approvalpath_uri']!= current_timeoff_approval_path['uri']:
        return {
            "uri": dag_run.conf['timeoff_approvalpath_uri'],
            "name": null
            }
    return null

def update_workweek(dag_run):
    current_workweek=rail.result('get_user_info')['userDetails']['workWeekStartDay']
    if not current_workweek and dag_run.conf['work_week_uri']:
        return {
            "workWeekStartDayUri": dag_run.conf['work_week_uri']
            }
    if dag_run.conf['work_week_uri'] and current_workweek['uri']!= dag_run.conf['work_week_uri']:
        return {
            "workWeekStartDayUri": dag_run.conf['work_week_uri']
            }
    return null

def update_timezone(dag_run):
    current_timezone = rail.result('get_user_info')['timeZone']
    if not current_timezone and dag_run.conf['timezone_uri']:
        return {
            "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
            "timezone": {
                "uri": dag_run.conf['timezone_uri'],
                "IANAName": null
            }
        }
    if dag_run.conf['timezone_uri'] and current_timezone['uri']!= dag_run.conf['timezone_uri']:
        return {
            "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
            "timezone": {
                "uri": dag_run.conf['timezone_uri'],
                "IANAName": null
            }
        }
    return null

def update_timesheet_period(dag_run):
    current_timesheet_period = rail.result("get_user_info")['timesheetPeriodSchedule']
    if not current_timesheet_period:
        return {
        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementTimesheetPeriodSchedule": [],
        "updateTimesheetPeriodScheduleOverDateRange": {
            "replacementTimesheetPeriodScheduleEntries": [
                {
                    "timesheetPeriod": {
                        "name": dag_run.conf['timesheet_period'],
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effective_date'])
                }
            ],
            "endDate": null
        }
    }

    if dag_run.conf['timesheet_period'] != current_timesheet_period[-1]['timesheetPeriod']['displayText']:
        return {
        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementTimesheetPeriodSchedule": [],
        "updateTimesheetPeriodScheduleOverDateRange": {
            "replacementTimesheetPeriodScheduleEntries": [
                {
                    "timesheetPeriod": {
                        "name": dag_run.conf['timesheet_period'],
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['effective_date'])
                }
            ],
            "endDate": null
        }
    }
    return null

def apply_user_modifications_payload(dag_run):
    update_user_payload =  {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timezoneToApply": update_timezone(dag_run),
            "workWeekStartToApply": update_workweek(dag_run),
            "holidayCalendarToApply": update_holiday_calendar(dag_run),
            "locationScheduleToApply": update_location_grp(dag_run.conf['location_uri'],
                rail.result('get_effective_user_groupmembership','location').get('uri', ''), dag_run),
            "departmentGroupScheduleToApply": update_department_grp(dag_run.conf['department_uri'],
                rail.result('get_effective_user_groupmembership', 'department').get('uri', ''), dag_run),
            "employeeTypeGroupScheduleToApply": update_employeetype_grp(dag_run.conf['employee_type_uri'],
                rail.result('get_effective_user_groupmembership', 'employeetype').get('uri', ''), dag_run),
            "timesheetPeriodScheduleToApply": update_timesheet_period(dag_run),
            "timesheetApprovalPathToApply": update_timesheet_approval_path(dag_run),
            "timeOffApprovalPathToApply": update_timeoff_approval_path(dag_run),
            "permissionSetsToApply": update_permission_set(dag_run),
            "policySetsToApply": update_policy_set(dag_run),
            "securitySettingsToApply": update_security_settings(dag_run),
            "customFieldValuesToApply": get_udfs('updateuser', dag_run),
            "userDetailsToApply": update_user_details(dag_run),
            "payRulesScheduleModifications": update_payrule_script(dag_run),
            },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

    return update_user_payload

def get_update_user_message(dag_run):
    exception_msg = []
    if dag_run.conf['payrule_name'] and not dag_run.conf['payrule_script_uri']:
        exception_msg.append("Payrule update skipped as payrule not found / disabled  in Replicon Instance.")
    if not dag_run.conf['payrule_name']:
        exception_msg.append("Payrule update skipped as Payrule not found for received combination in mapper")
    if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        exception_msg.append('Supervisor is disabled in replicon')

    if exception_msg:
        return f"User Partially updated; {';'.join(exception_msg)}"

    if get_task_state('log_supervisor_not_present') == 'success':
        return ""
    return "User Updated Successfully"

def get_update_user_severity(dag_run):
    if not dag_run.conf['payrule_script_uri'] or get_task_state(
        'log_supervisor_not_present') == 'success' or get_task_state(
        'log_supervisor_disabled_in_replicon') == 'success':
        return 'Exception'
    return 'Success'
