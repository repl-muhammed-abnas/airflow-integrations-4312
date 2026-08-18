from datetime import datetime
from pendulum import now
import json
from momentive.user_import_india.utils import python_callable
import rail
null = None


def effective_dateformat_payload(effective_date, split_type='datetime'):
    if split_type == 'int':
        return {
            "year": int(effective_date.strftime("%Y")),
            "month": int(effective_date.strftime("%m")),
            "day": int(effective_date.strftime("%d"))
        }
    return {
        "year": effective_date.year,
        "month": effective_date.month,
        "day": effective_date.day
    }


def get_user_by_search_payload(text_search_term):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:end-date",
            "urn:replicon:user-list-column:start-date",
            "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": text_search_term
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_data_sup_emp_grp_dept_grp(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:department-group",
            "urn:replicon:user-list-column:employee-type-group",
            "urn:replicon:user-list-column:supervisor"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:user"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "uri": dag_run.conf['useruri']
                }
            }
        }
    }


def search_supervisor_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:login-name"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['managerid']
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def supervisor_assignment_log_payload(dag_run):
    return {
        "jobid": dag_run.conf['parentjobid'],
        "loginid": dag_run.conf['userid'],
        "supervisorempid": dag_run.conf['managerid'],
        "useruri": dag_run.conf['useruri'] if 'useruri' in dag_run.conf else rail.result('create_user')['uri'],
        'type': "update" if 'useruri' in dag_run.conf else "add",
        "sup_email": dag_run.conf['CF_LRV_Manager_Email'] if dag_run.conf['CF_LRV_Manager_Email'] else '',
        "sup_firstname": dag_run.conf['CF_LRV_Manager_First_Name'] if dag_run.conf['CF_LRV_Manager_First_Name'] else '',
        "sup_lastname": dag_run.conf['CF_LRV_Manager_Last_Name'] if dag_run.conf['CF_LRV_Manager_Last_Name'] else '',
        "sup_change_effective_date": dag_run.conf['effective_date_of_manager_change']
        if dag_run.conf['effective_date_of_manager_change'] else str(datetime.strftime(datetime.now().date(), '%Y-%m-%d')),
    }


def add_missing_supervisor_permission_payload():
    return {
        'userUri': rail.result('search_for_user_with_empid')[0]['uri'],
        'permissionSetUri': rail.result('get_all_permissionsets')['supervisor']
    }


def get_current_supervisorempid():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:user-list-column:employee-id"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:user"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "uri": rail.result('search_for_user_with_empid')[0]['uri']
                }
            }
        }
    }


def search_location_department_group_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:effectively-enabled",
            "urn:replicon:department-group-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:department-group-list-filter:text"
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
                    "text": dag_run.conf['location'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_timesheet_for_date2_payload(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "date": effective_dateformat_payload(datetime.now()),
        "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
    }


def update_employeetypegrp_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "employeeTypeGroupScheduleToApply": {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {
                                "uri": rail.result('get_all_employee_type')
                            },
                            "effectiveDate": effective_dateformat_payload(datetime.now())
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def department_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "departmentGroupScheduleToApply": {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": {
                                "uri": dag_run.conf['departmentgroupuri']
                            },
                            "effectiveDate": python_callable.split_date_string(dag_run.conf['CF_LRV_Location_Change_Effective_Date'],
                                                                               'datetime') if dag_run.conf['CF_LRV_Location_Change_Effective_Date'] else effective_dateformat_payload(datetime.now())
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def schedule_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "schedulePolicyToApply": {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [{
                        "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": null,
                            "officeSchedule": null,
                            "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                        } if rail.result('search_entry_in_mapper_for_schedule') == 'Shift' else {
                            "officeScheduleUri": rail.result('get_req_schedule_script'),
                            "officeSchedule": {
                                "officeScheduleUri": rail.result('get_req_schedule_script'),
                            },
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate": python_callable.split_date_string(dag_run.conf['work_shift_change_effective_date'], 'datetime')
                        if dag_run.conf['work_shift_change_effective_date'] else effective_dateformat_payload(datetime.now())
                    }]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def assign_policydataaccessscope_department(dag_run):
    return {
        "userUri": dag_run.conf['useruri'] if 'useruri' in dag_run.conf else rail.result('create_user')['uri'],
        "policyDataAccessScopes": [{
            "policyUri": "urn:replicon:policy:time-off",
            "departmentGroups": [{
                "departmentGroup": {
                    "uri": dag_run.conf['departmentgroupuri']
                }
            }]
        }]
    }


def trigger_updateuser_timeoff(dag_run):
    strt_date = rail.result('get_user_data')[
        0]['userDetails']['employmentDateRange']['startDate']
    return {
        "parentjobid": dag_run.conf['parentjobid'],
        "userid": dag_run.conf['userid'],
        "hiredate": dag_run.conf['hiredate'],
        "terminationdate": dag_run.conf['terminationdate'],
        "active": dag_run.conf['active'],
        "rehire": dag_run.conf['rehire_update'],
        "timeofftypes": rail.result('search_mapper_for_timeoff_types'),
        "old_startdate": str(strt_date['year']) + '-' + str(strt_date['month']) + '-' + str(strt_date['day']),
        "useruri": dag_run.conf['useruri'],
        "exemptionstatus": dag_run.conf['exemptionstatus'],
        "businesstitle": rail.result('log_businesstitle_127'),
        "workertype": dag_run.conf['workertype'],
        "location": dag_run.conf['location'],
    }


def trigger_timeoff_add_new_user(dag_run):
    return {
        "parentjobid": rail.render_template("{{dag_run_ecid()}}"),
        "firstname": dag_run.conf['firstname'],
        "lastname": dag_run.conf['lastname'],
        "loginname": dag_run.conf['userid'],
        "employeeid": dag_run.conf['Worker_Reference_Employee_ID'],
        "supervisor": dag_run.conf['managerid'],
        "emailaddress": dag_run.conf['emailaddress'],
        "startdate": dag_run.conf['hiredate'],
        "useruri": rail.result('create_user')['uri'],
        "terminationdate": dag_run.conf['terminationdate'],
        "workertype": dag_run.conf['workertype'],
        "effective_date_of_worker_type": dag_run.conf['effective_date_of_worker_type'],
        "exemptionstatus": dag_run.conf['exemptionstatus'],
        "exemption_eff_date": dag_run.conf['exemption_eff_date'],
        "gender": dag_run.conf['gender'],
        "active": dag_run.conf['active'],
        "timeofftypes": rail.result('log_timeoff_typestobeassigned_129')['value'],
        "rehire": 'add'
    }


def get_manager_details_payload():
    return {
        "users": [
            {
                "uri": rail.result('search_for_user_with_empid')[0]['uri']
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def create_supervisor_payload(dag_run):
    return {
        "user": {
            "target": {
                "loginName": dag_run.conf['sup_email']
            },
            "firstname": dag_run.conf['sup_firstname'],
            "lastname": dag_run.conf['sup_lastname'],
            "emailAddress": dag_run.conf['sup_email'],
            "employeeId": dag_run.conf['managerid'],
            "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
            "employmentDateRange": {
                "startDate": python_callable.split_date_string(dag_run.conf['sup_change_effective_date'], 'datetime')
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['sup_email'],
                "SSOName": dag_run.conf['sup_email'],
                "password": "Replicon@12"
            },
            "permissionSets": [
                {
                    "uri": rail.result('get_all_permissionsets')['supervisor']
                }
            ],
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "name": "Momentive",
                    }
                }
            ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "name": "Foreign Supervisors",
                    }
                }
            ]
        }
    }


def final_policyset_schedule_entry(dag_run):
    final_entries = rail.result("get_past_policysetschedule_entries")
    end_date_string_split = {
        'day': dag_run.conf['terminationdate'].split("/")[0],
        'month': dag_run.conf['terminationdate'].split("/")[1],
        'year': dag_run.conf['terminationdate'].split("/")[2]
    }
    final_entries.append({
        "effectiveDate": end_date_string_split,
        "description": "Effective on " + end_date_string_split['month'] + "/" + end_date_string_split['day'] + "/" + end_date_string_split['year'],
        "policySet": {
            "timeOffBalanceEventScripts": [
                {
                    "scriptTarget": {
                        "uri": dag_run.conf['startingbalancesettouri'],
                        "slug": null,
                        "name": null
                    },
                    "additionalParameters": [
                        {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": rail.get_dag_run_var('balance_amount'),
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        },
                        {
                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                            "value": {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": "20",
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        }
                    ]
                }
            ],
            "timeOffValidationScripts": []
        }
    })

    return final_entries


def conf_payload(action):
    conf = {
        "parentjobid": rail.render_template("{{ dag_run_ecid() }}"),
        "userid": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['User_ID'],
        "Worker_Reference_Employee_ID": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Worker_Reference_Employee_ID'],
        "emailaddress": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Email_Address'],
        "firstname": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['First_Name'],
        "lastname": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Last_Name'],
        "workertype": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Worker_Type'],
        "effective_date_of_worker_type": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Effective_Date_of_Worker_Type'] if rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Effective_Date_of_Worker_Type'] else null,
        "exemptionstatus": "Yes" if "1" in str(rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Exemption_Status']) else "No",
        "exemption_eff_date": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Exemption_Eff_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Exemption_Eff_Date'] else null,
        "gender": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Gender'],
        "hiredate": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Hire_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Hire_Date'] else null,
        "terminationdate": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Termination_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Termination_Date'] else null,
        "active": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Active'],
        "function": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Function'],
        "function_change_effective_date": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Function_Change_Effective_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Function_Change_Effective_Date'] else null,
        "businesstitle": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Business_Title'] or null,
        "CF_LRV_Business_Title_Change_Eff_Date": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['CF_LRV_Business_Title_Change_Eff_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_21_27')['CF_LRV_Business_Title_Change_Eff_Date'] else null,
        "fieldhr": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Field_HR'],
        "managerid": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Manager_ID'],
        "effective_date_of_manager_change": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Effective_Date_of_Manager_Change'] if rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Effective_Date_of_Manager_Change'] else null,
        "workshift": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Work_Shift'],
        "work_shift_change_effective_date": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Work_Shift_Change_Effective_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Work_Shift_Change_Effective_Date'] else null,
        "location": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Location'],
        "CF_LRV_Location_Change_Effective_Date": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['CF_LRV_Location_Change_Effective_Date'] if rail.result('foreach_query_list_usershereloginnameispresent_21_27')['CF_LRV_Location_Change_Effective_Date'] else null,
        "country": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['Country'],
        "CF_Date_of_Birth_MM_DD_YYYY": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['CF_Date_of_Birth_MM_DD_YYYY'] if rail.result('foreach_query_list_usershereloginnameispresent_21_27')['CF_Date_of_Birth_MM_DD_YYYY'] else null,
        "CF_LRV_Manager_Email": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['CF_LRV_Manager_Email'],
        "CF_LRV_Manager_First_Name": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['CF_LRV_Manager_First_Name'],
        "CF_LRV_Manager_Last_Name": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['CF_LRV_Manager_Last_Name'],
        "departmentgroupuri": rail.result('log_ifuserexistsuseruri_and_departmentgroupuri_36_37')['departmentgroupuri'],
        "India_Spec_schedule_Indicator": rail.result('foreach_query_list_usershereloginnameispresent_21_27')['India_Spec_schedule_Indicator'] or null,
        "user_import_logs": rail.result('create_log_momentive_user_import_log'),
        "supervisor_assignment_logs": rail.result('create_log_momentive_supervisor_assignment')
    }

    if action == 'rehire':
        conf.update({"rehire_update": "rehire", "useruri": rail.result(
            'log_ifuserexistsuseruri_and_departmentgroupuri_36_37')['useruri']})
    elif action == 'update' or action == 'disable':
        conf.update({"rehire_update": "update", "useruri": rail.result(
            'log_ifuserexistsuseruri_and_departmentgroupuri_36_37')['useruri']})

    return conf


def dict_to_datetime(dict_date):
    return datetime(day=dict_date['day'], month=dict_date['month'], year=dict_date['year'])


def get_current_value_from_schedule_list_for_user(user_schedule, scrpit_name, required_key):
    current_value = null
    initial_value = null
    current_min_day_diff = "*"
    if 'urn' in json.dumps(user_schedule):
        for item in user_schedule:

            if not (item['effectiveDate']):
                initial_value = item
                continue

            daydiff = (now().date()) - \
                dict_to_datetime((item['effectiveDate'])).date()

            # ignore the future ones
            if daydiff.days < 0:
                continue

            if current_min_day_diff == "*":
                current_value = item
                current_min_day_diff = daydiff
                continue

            if current_min_day_diff > daydiff:
                current_min_day_diff = daydiff
                current_value = item

    return ((current_value[scrpit_name][required_key] if current_value['scheduleTypeUri'] != "urn:replicon:schedule-type:shift" else "Shift") if current_value else (
        (initial_value[scrpit_name][required_key] if initial_value['scheduleTypeUri'] != "urn:replicon:schedule-type:shift" else "Shift") if initial_value else '')) if scrpit_name == "officeSchedule" else (
            current_value[scrpit_name][required_key]) if current_value else (
                initial_value[scrpit_name][required_key] if initial_value else '')
