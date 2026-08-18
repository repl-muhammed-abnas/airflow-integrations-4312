# pylint: disable=too-many-branches too-many-statements
from datetime import datetime
from hashlib import md5
import uuid
import rail
from rail.lib.ecid import get_dagrun_ecid

null = None
DATE_FORMAT = "%m/%d/%Y"

def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def user_import_csv_data(item):
    return [
        item['First Name'],
        item['Last Name'],
        item['Login Name'],
        item['Employee ID'],
        item['Email'],
        item['EmployeeType'],
        item['Authentication Type'],
        item['Cost Center'],
        item['Business Unit or Group'],
        item['Is Login Enabled'],
        item['Start Date'],
        item['End Date'],
        item['Level'],
        item['Manager'],
        item['Location/Office'],
        item['User Permission'],
        item['Supervisor Permission'],
        item['Team Manager Permission'],
        item['Payroll Manager Permission'],
        item['Administrator Permission'],
        item['Licenses'],
        item['Timesheet Template'],
        item['Timesheet Approval Path'],
        item['Timesheet Period'],
        item['Schedule'],
        md5(",".join([item['First Name'],item['Last Name'],item['Login Name'],item['Employee ID'],
            item['Email'],item['EmployeeType'],item['Authentication Type'],item['Cost Center'],
            item['Business Unit or Group'],item['Is Login Enabled'],item['Start Date'],
            item['End Date'],item['Level'],item['Manager'],item['Location/Office'],
            item['User Permission'],item['Supervisor Permission'],item['Team Manager Permission'],
            item['Payroll Manager Permission'],item['Administrator Permission'],item['Licenses'],item['Timesheet Template'],
            item['Timesheet Approval Path'],item['Timesheet Period'],
            item['Schedule']]).encode()).hexdigest()
    ]

MANDATORY_FIELDS = {
    "firstname": "First Name",
    "lastname": "Last Name",
    "loginname":"Login Name",
    "authtype": "Authentication Type",
    "employeeid": "EmployeeId"
}

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")

def get_userlist_report_params():
    return {
        "reportParameters": [{
            "filterValues": [],
            "outputFormatUri": "urn:replicon:report-output-format-option:csv",
            "reportUri": rail.result('get_report_uri')['userlist_report_uri']
        }
        ]
    }

def get_enabled_timesheet_period():
    return {
            "page": "1",
            "pagesize": "10000000",
            "columnUris": [
                "urn:replicon:timesheet-period-list-column:timesheet-period",
                "urn:replicon:timesheet-period-list-column:enabled"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "filterDefinitionUri": "urn:replicon:timesheet-period-list-filter:enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                "value": {
                    "bool": "true"
                }
                }
            }
        }

def get_enabled_locations():
    return {
            "page": "1",
            "pagesize": "10000",
            "columnUris": [
                "urn:replicon:location-list-column:location",
                "urn:replicon:location-list-column:code"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "filterDefinitionUri": "urn:replicon:location-list-filter:effectively-enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                "value": {
                    "bool": "true"
                }
                }
            }
        }

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

def get_add_department_payload(dag_run):
    return {
        "departmentGroup": {
            "parent": {
                "uri": rail.result("get_parent_department_details")[0]['uri']
            },
        } if rail.result('get_parent_department_details') else null,
        "modifications": {
            "name": dag_run.conf['department_name'],
            "isEnabled": "1"
        },
        "unitOfWorkId": str(uuid.uuid4())
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
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_add_location_payload(dag_run):
    return {
        "location": {
            "parent": {
                "uri": rail.result("get_parent_location_details")[0]['uri']
                }
        } if rail.result("get_parent_location_details") else null,
        "modifications": {
            "name": dag_run.conf['location_name'],
            "codeToApply": null,
            "descriptionToApply":null,
            "isEnabled": "true"
        },
        "unitOfWorkId": str(uuid.uuid4()),
    }

def get_add_division_payload(dag_run):
    return {
        "division": {
            "parent": {
                "uri": rail.result("get_parent_division_details")[0]['uri']
                }
        } if rail.result("get_parent_division_details") else null,
        "modifications": {
            "name": dag_run.conf['division_name'],
            "codeToApply": null,
            "isEnabled": 1
        },
        "unitOfWorkId": str(uuid.uuid4()),
    }

def get_process_users_conf(item):
    get_user_udfs = rail.result('get_user_udfs')

    def get_all_permissionseturis(item):
        permissionsets = []
        replicon_permission_set = rail.result('get_all_permission_set')
        if item['userpermission']:
            permissionsets.append({
                'name': item['userpermission'],
                'uri': rail.find_first_by_attr_and_get_attr(replicon_permission_set,'displayText',item['userpermission'],'uri')
            })
        if item['supervisorpermission']:
            permissionsets.append({
                'name': item['supervisorpermission'],
                'uri': rail.find_first_by_attr_and_get_attr(replicon_permission_set,'displayText',item['supervisorpermission'],'uri')
            })
        return permissionsets or None
    return {
        **item,
        **{
            'leveluri': get_user_udfs['leveluri'],
            'departmenturi':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_departments'), 'full_path', item['costcenter'], 'uri'),
            'employeetypeuri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_employee_types'), 'full_path', item['employeetype'], 'uri'),
            'locationuri':rail.find_first_by_attr_and_get_attr(rail.result('get_updated_locations'), 'name', item['location'], 'uri'),
            'divisionuri': rail.find_first_by_attr_and_get_attr(rail.result('get_updated_buisness_unit_grps'), 'name',
                item['businessunitorgroup'], 'uri'),
            'permissionsetdetails': get_all_permissionseturis(item),
            'supervisor_permission_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'), 'name', 'Supervisor', 'uri'),
            'timesheettemplateuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_sets"),'displayText',item['timesheettemplate'],"uri")
                if item['timesheettemplate'] else null,
            'timesheetapprovalpathuri': rail.find_first_by_attr_and_get_attr(rail.result("get_timesheet_approval_paths"),'displayText',
                item['timesheetapprovalpath'],"uri") if item['timesheetapprovalpath'] else null,
            'officescheduleuri': rail.find_first_by_attr_and_get_attr(rail.result("get_default_office_schedule"),'displayText',item['schedule'],'uri',''),
            'timesheetperioduri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_timesheet_period_list"),'name',item['timesheetperiod'],'uri',''),
            'supervisor_log' : rail.result('create_supervisor_log'),
            'jobid': get_dagrun_ecid(rail.get_current_context()['dag_run'])
        }
    }

def get_user_data_payload(dag_run):
    return {
        "users": [
            {
            "uri": null,
            "loginName": null,
            "employeeId": dag_run.conf['employeeid'],
            "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_supervisor_data_payload(dag_run):
    return {
        "users": [
            {
            "uri": null,
            "loginName": null,
            "employeeId": dag_run.conf['manager'],
            "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
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
    if not get_replicon_date(dag_run.conf['startdate']):
        return False
    if dag_run.conf['enddate']:
        if not  get_replicon_date(dag_run.conf['enddate']):
            return False
    return True

def get_invalid_fields_message(dag_run):
    log=[]
    if not get_replicon_date(dag_run.conf['startdate']):
        log.append('Invalid Date format for Start Date')
    if dag_run.conf['enddate']:
        if not get_replicon_date(dag_run.conf['enddate']):
            log.append('Invalid Date format for End Date')
    return rail.smartjoin_by_delim(log,";")

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
            'useruri': rail.result('get_user_data')[0]['userDetails']['uri'],
            'todaysdate': (datetime.now()).strftime(DATE_FORMAT)
        }
    }

def add_permission_sets(log, dag_run):
    all_permission_sets = dag_run.conf['permissionsetdetails']

    if not all_permission_sets:
        return null

    permission_set_uri_not_available = list(filter(lambda x:x['uri']== null, all_permission_sets))
    if len(permission_set_uri_not_available) > 0:
        log.append(f"""Permission set - {rail.smartjoin_by_delim([item['name'] for item in permission_set_uri_not_available], ";")
            } not available in Replicon""")

    permission_set_uri_available = list(filter(lambda x:x['uri']!= null, all_permission_sets))
    if len(permission_set_uri_available) > 0:
        return list(map(lambda item:{
             "uri": item['uri'],
             "name": null
        }, permission_set_uri_available))

    return null

def get_policy_sets(log, dag_run):
    policy_set = []
    if dag_run.conf['timesheettemplate'] and not dag_run.conf['timesheettemplateuri']:
        log.append(f"Timesheet Template - {dag_run.conf['timesheettemplate']} is not available in Replicon")

    if not dag_run.conf['timesheettemplateuri']:
        return null

    if dag_run.conf['timesheettemplateuri']:
        policy_set.append({
                    "uri": dag_run.conf['timesheettemplateuri'],
                    "name": null
                })
    return policy_set

def get_timesheet_approvalpath(log, dag_run):
    if not dag_run.conf['timesheetapprovalpath']:
        return null
    if dag_run.conf['timesheetapprovalpath'] and not dag_run.conf['timesheetapprovalpathuri']:
        log.append(f"Timesheet Approval Path - {dag_run.conf['timesheetapprovalpath']} is not available in Replicon")
        return null
    return {
            "uri": dag_run.conf['timesheetapprovalpathuri'],
            "name": null
        }

def get_udfs(userstatus, dag_run):
    udfs = []
    def add_udf_field_values(definitionuri, textvalue = null , number = null):
        if definitionuri and textvalue:
            udfs.append({
                "customField": {
                "uri": definitionuri,
                "name": null,
                "groupUri": null
                },
                "text": textvalue,
                "date": null,
                "dropDownOption": null,
                "number": number
            })

    if userstatus =='adduser':
        if dag_run.conf['level']:
            add_udf_field_values(definitionuri = dag_run.conf['leveluri'], textvalue = dag_run.conf['level'])

    if userstatus == 'updateuser':
        current_level = rail.find_first_by_attr_and_get_attr(rail.result('get_current_udf_values'),
            'customField.displayText', 'Level', 'text')

        if dag_run.conf['level'] and dag_run.conf['level'] != current_level:
            add_udf_field_values(definitionuri = dag_run.conf['leveluri'], textvalue = dag_run.conf['level'])

    return udfs

def get_put_user_payload(dag_run):
    log=[]
    put_user_payload = {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['loginname'],
            },
            "firstname": dag_run.conf['firstname'],
            "lastname": dag_run.conf['lastname'],
            "emailAddress": dag_run.conf['email'],
            "employeeId": dag_run.conf['employeeid'],
            "employmentDateRange": {
                "startDate": get_replicon_date(dag_run.conf['startdate']),
                "endDate": get_replicon_date(dag_run.conf['enddate']) if dag_run.conf['enddate'] else null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                   "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true" if dag_run.conf['isloginenable'] == "Yes" else "false",
                "loginName": dag_run.conf['loginname']
            },
            "permissionSets": add_permission_sets(log, dag_run),
            "policySets": get_policy_sets(log, dag_run),
            "payrollRateSchedule": null,
            "timesheetApprovalPath": get_timesheet_approvalpath(log,dag_run),
            "customFieldValues": get_udfs('adduser', dag_run),
            "assignedActivities": [],
            "timeZone": null,
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [
                {
                    "location": {
                        "uri": dag_run.conf['locationuri'],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['locationuri'] else [],
            "divisionSchedule":  [
                {
                    "division": {
                    "uri": dag_run.conf['divisionuri'],
                    "parentUri": null,
                    "name": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['divisionuri'] else [],
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "uri": dag_run.conf['departmenturi'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['departmenturi'] else [],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": dag_run.conf['employeetypeuri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['employeetypeuri'] else [],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "uri": dag_run.conf['timesheetperioduri'],
                        "name": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf['timesheetperioduri'] else [],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [],
            "displayNameParameter": null,
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": []
        }
    }

    rail.set_result(key="exception_logs",val= log)

    return put_user_payload

def get_add_user_message():
    # pylint: disable=too-many-return-statements
    if get_task_state('log_supervisor_not_present') == 'success':
        return "Supervisor not present"
    if get_task_state('update_supervisor_for_user') == 'success':
        return "User Added"
    if get_task_state('log_user_supervisor_same') == 'success':
        return "Employee and Supervisor is same"
    return "User Partially Added"


def get_add_user_severity():
    if get_task_state('log_supervisor_not_present') == 'success':
        return 'Exception'
    if get_task_state('log_user_supervisor_same') == 'success':
        return 'Exception'
    return 'Success'

def validate_supervisor_changed():
    if not rail.result('get_effective_supervisor_of_user'):
        return True
    if rail.result('search_supervisor_in_replicon') and rail.result('get_effective_supervisor_of_user') and \
        rail.result('search_supervisor_in_replicon')['loginname'] == rail.result('get_effective_supervisor_of_user')['supervisor']['user']['loginName']:
        return False
    return True

def get_supervisor_status(dag_run):
    if get_task_state('log_supervisor_not_present') == 'success' \
        or get_task_state('log_supervisor_disabled_in_replicon') == 'success' or dag_run.conf['exception_logs']:
        return 'Exception'
    return 'Success'

def get_supervisor_message(action, dag_run):
    # pylint: disable=too-many-return-statements
    exception_log = dag_run.conf['exception_logs'] if dag_run.conf['exception_logs'] else []
    if get_task_state('log_supervisor_not_present') == 'success':
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + \
            ',Supervisor not present in replicon;'+ rail.smartjoin_by_delim(exception_log, ";")
    if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return ("User Partially Added" if action == 'Add' else "User Partially Updated") + ',Supervisor is disabled in replicon;'
    return f"""User {('Added' if action=='add' else 'Updated')
        if not exception_log else ('Partially Added,'if action=='add' else 'Partially Updated,') + rail.smartjoin_by_delim(exception_log, ";")}"""

def get_supervisor_permission_uri():
    all_permissionsets = rail.result('get_all_permission_set')
    uri = rail.find_first_by_attr_and_get_attr(all_permissionsets, 'name', 'Supervisor', 'uri')
    return uri

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
                    "effectiveDate": get_replicon_date(dag_run.conf['todaysdate'])
                }
            ],
            "endDate": null
        }
    } if locationuri and currentlocationuri != locationuri else null

def update_buisness_unit_grp(buisness_unit_uri, current_buisness_unit_uri, dag_run):
    return {
        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementDivisionSchedule": [],
        "updateDivisionScheduleOverDateRange": {
            "replacementDivisionScheduleEntries": [
                {
                    "division": {
                        "uri": buisness_unit_uri
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['todaysdate'])
                }
            ],
            "endDate": null
        }
    } if buisness_unit_uri and buisness_unit_uri != current_buisness_unit_uri else null

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
                    "effectiveDate": get_replicon_date(dag_run.conf['todaysdate'])
                }
            ],
            "endDate": null
        }
    } if departmenturi and departmenturi != currentdepartmenturi else null

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
                    "effectiveDate": get_replicon_date(dag_run.conf['todaysdate'])
                }
            ],
            "endDate": null
        }
    } if employeetypeuri and employeetypeuri != currentemployeetypeuri else null

def update_permission_set(log, dag_run):
    permission_set_uris = []
    all_permission_sets = dag_run.conf['permissionsetdetails']

    if not all_permission_sets:
        return null

    permission_set_uri_not_available = list(filter(lambda x:x['uri']== null, all_permission_sets))
    if len(permission_set_uri_not_available) > 0:
        log.append(f"""Permission set - {rail.smartjoin_by_delim([item['name'] for item in permission_set_uri_not_available], ";")
            } not available in Replicon""")

    permission_set_uri_available = list(filter(lambda x:x['uri']!= null, all_permission_sets))
    if len(permission_set_uri_available) > 0:
        for item in permission_set_uri_available:
            if not rail.find_first_by_attr_and_get_attr(rail.result('get_user_info')[0]['permissionSets'],
            'displayText', item['name'], 'displayText'):
                permission_set_uris.append(item['uri'])
    return {
            "permissionSetUrisToAssign": permission_set_uris,
            "policyUrisToRemovePermissionSet": []
        } if permission_set_uris else null

def update_user_details(dag_run):
    user_details = rail.result("get_user_info")[0]['userDetails']
    return {
      "firstName": dag_run.conf['firstname'] if user_details['firstName'] != dag_run.conf['firstname'] else null,
      "lastName": dag_run.conf['lastname'] if user_details['lastName'] != dag_run.conf['lastname'] else null,
      "language": null,
      "employmentDateRange": null,
      "employmentStartDate": {
        "date": get_replicon_date(dag_run.conf['startdate'])
      } if user_details['employmentDateRange']['startDate'] != get_replicon_date(dag_run.conf['startdate']) else null,
       "employmentEndDate": {
         "date": get_replicon_date(dag_run.conf['enddate']) if bool(get_replicon_date(dag_run.conf['enddate'])) else null
       },
       "emailAddress": {
            "emailAddress": dag_run.conf['email']
        } if user_details['emailAddress'] != dag_run.conf['email'] else null,
    }

def update_policy_set(log, dag_run):
    assigned_timesheet_template = rail.result("get_user_info")[0]['timesheetTemplate']
    policy_set = []

    if dag_run.conf['timesheettemplate'] and not dag_run.conf['timesheettemplateuri']:
        log.append(f"Timesheet Template - {dag_run.conf['timesheettemplate']} is not available in Replicon")

    if dag_run.conf['timesheettemplateuri']:
        if not assigned_timesheet_template or (assigned_timesheet_template and (
            dag_run.conf['timesheettemplate'] != assigned_timesheet_template['displayText'])):
            policy_set.append(dag_run.conf['timesheettemplateuri'])

    return {
            "policySetUrisToAssign": policy_set,
            "policyUrisToRemovePolicySet": []
        } if policy_set else null

def update_timesheet_approvalpath(log, dag_run):
    if dag_run.conf['timesheetapprovalpath'] and not dag_run.conf['timesheetapprovalpathuri']:
        log.append(f"Timesheet Approval Path - {dag_run.conf['timesheetapprovalpath']} is not available in Replicon")
    current_timesheet_approvalpath = rail.result('get_user_info')[0]['timesheetApprovalPath']

    if dag_run.conf['timesheetapprovalpathuri']:
        if not current_timesheet_approvalpath or (current_timesheet_approvalpath and (
            dag_run.conf['timesheetapprovalpath'] != current_timesheet_approvalpath['displayText'])):
            return {
                    "uri": dag_run.conf['timesheetapprovalpathuri'],
                    "name": null
                }
    return null

def update_timesheet_period(timesheetperiod, current_timesheet_period, dag_run):

    return {
        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
        "replacementTimesheetPeriodSchedule": [],
        "updateTimesheetPeriodScheduleOverDateRange": {
            "replacementTimesheetPeriodScheduleEntries": [
                {
                    "timesheetPeriod": {
                        "name": timesheetperiod,
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['todaysdate'])
                }
            ],
            "endDate": null
        }
    } if timesheetperiod and current_timesheet_period and current_timesheet_period[-1]['timesheetPeriod']['displayText']!= timesheetperiod else null


def apply_user_modifications_payload(dag_run):
    log = []
    update_user_payload =  {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timezoneToApply": null,
            "locationScheduleToApply": update_location_grp(dag_run.conf['locationuri'],
                rail.result('get_effective_user_groupmembership','location').get('uri', ''), dag_run),
            "divisionScheduleToApply": update_buisness_unit_grp(dag_run.conf['divisionuri'],
                rail.result('get_effective_user_groupmembership', 'division').get('uri', ''), dag_run),
           "departmentGroupScheduleToApply": update_department_grp(dag_run.conf['departmenturi'],
                rail.result('get_effective_user_groupmembership', 'department').get('uri', ''), dag_run),
            "employeeTypeGroupScheduleToApply": update_employeetype_grp(dag_run.conf['employeetypeuri'],
                rail.result('get_effective_user_groupmembership', 'employeetype').get('uri', ''), dag_run),
            "permissionSetsToApply": update_permission_set(log, dag_run),
            "policySetsToApply": update_policy_set(log, dag_run),
            "timesheetPeriodScheduleToApply": update_timesheet_period(dag_run.conf['timesheetperiod'],
                rail.result("get_user_info")[0]['timesheetPeriodSchedule'], dag_run),
            "timesheetApprovalPathToApply": update_timesheet_approvalpath(log, dag_run),
            "customFieldValuesToApply": get_udfs('updateuser', dag_run),
            "userDetailsToApply": update_user_details(dag_run),
            "payRulesScheduleModifications": null,
            "payrollRatesModifications": null,
            },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

    rail.set_result(key="exception_logs",val= log)

    return update_user_payload

def get_update_user_message():
    # pylint: disable=too-many-return-statements
    if get_task_state('log_supervisor_not_present') == 'success':
        return "Supervisor not present"
    if get_task_state('log_user_supervisor_same') == 'success':
        return "Employee and Supervisor is same"
    exception_logs = rail.result('apply_user_modifications', 'exception_logs')
    if not exception_logs:
        if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
            return 'User Partially Updated, Supervisor is disabled in replicon'
        return "User Updated"
    if get_task_state('log_supervisor_disabled_in_replicon') == 'success':
        return 'User Partially Updated, Supervisor is disabled in replicon'+ rail.smartjoin_by_delim(exception_logs, ";")
    return "User Partially Updated,"+ rail.smartjoin_by_delim(exception_logs, ";")

def get_update_user_severity():
    if get_task_state('log_supervisor_not_present') == 'success' or get_task_state('log_user_supervisor_same') == 'success'\
        or get_task_state('log_supervisor_disabled_in_replicon') == 'success' or rail.result('apply_user_modifications', 'exception_logs'):
        return 'Exception'
    return 'Success'

def validate_enddate(dag_run):
    if dag_run.conf['startdate'] and dag_run.conf['enddate']:
        return datetime.strptime(dag_run.conf['enddate'], DATE_FORMAT) > datetime.strptime(dag_run.conf['startdate'], DATE_FORMAT)
    return False
