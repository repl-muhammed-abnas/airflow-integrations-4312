# pylint: disable=unused-variable too-many-branches too-many-statements
from datetime import datetime, timedelta
from functools import lru_cache
from hashlib import md5
import rail


def get_today_dateformat_payload():
    return {
        "year": datetime.now().strftime("%Y"),
        "month": datetime.now().strftime("%m"),
        "day": datetime.now().strftime("%d")
    }


def effective_dateformat_payload(effective_date):
    return {
        "year": effective_date.year,
        "month": effective_date.month,
        "day": effective_date.day
    }


def user_import_csv_data(item):
    return [
        item['FirstName'],
        item['LastName'],
        item['UserName'],
        item['WorkEmail'],
        item['EmplID'],
        item['EmployeeType'],
        item['HireDate'],
        item['TermDate'],
        item['ManagerName'],
        item['Department'],
        item['Location'],
        item['SubstituteName'],
        item['Timezone'],
        item['ScheduledHours'],
        item['ManagementLevel'],
        item['Division'],
        item['Position'],
        item['HomeWorkStatee'],
        item['EmployeeStatus'],
        item['Approver'],
        md5("".join([str(item['FirstName']), str(item['LastName']), str(item['UserName']), str(item['WorkEmail']), str(item['EmplID']),
                    str(item['EmployeeType']), str(item['HireDate']), str(
                        item['TermDate']), str(item['ManagerName']), str(item['Department']),
                    str(item['Location']), str(item['SubstituteName']), str(
                        item['Timezone']), str(item['ScheduledHours']),
                    str(item['ManagementLevel']), str(item['Division']), str(
                        item['Position']), str(item['HomeWorkStatee']),
                    str(item['EmployeeStatus']), str(item['Approver'])]).encode()).hexdigest()
    ]


def get_customfield_dropdown_option_uris():
    existing_dropdowns_list = rail.result('get_all_customfield_dropdowns')
    final_dropdown_list = list(map(lambda x: {
        'target': {
            'uri': x['uri'],
            'name': x['displayText']
        },
        'name': x['displayText'],
        'isEnabled': x['isEnabled']
    }, existing_dropdowns_list)) if existing_dropdowns_list else []

    new_values_to_set = rail.load_all_records(
        rail.result('new_managementlevel_values'))

    final_dropdown_list.extend(map(lambda x: {
        'name': x['managementlevel'],
        'isEnabled': True
    }, new_values_to_set))

    return final_dropdown_list


def get_customfield_dropdown_option(dag_run):
    existing_dropdowns_list = rail.result('get_all_customfield_dropdowns')
    final_dropdown_list = list(map(lambda x: {
        'target': {
            'uri': x['uri'],
            'name': x['displayText']
        },
        'name': x['displayText'],
        'isEnabled': x['isEnabled']
    }, existing_dropdowns_list)) if existing_dropdowns_list else []

    final_dropdown_list.append({
        'name': dag_run.conf['udf_value'],
        'isEnabled': True
    })

    return final_dropdown_list


def update_emp_date_for_disableuser(dag_run):
    startdate = rail.result('get_user_details_for_disable')[
        0]['userDetails']['employmentDateRange']['startDate']
    termdate = datetime.strptime(dag_run.conf['termdate'], '%d-%b-%Y')
    return {
        "userUri": dag_run.conf['useruri'],
        "dateRange": {
            "startDate": {
                "year": startdate['year'],
                "month": startdate['month'],
                "day": startdate['day']
            },
            "endDate": effective_dateformat_payload(termdate)
        }
    }


def get_put_timeoffpolicywithinitialbalance(dag_run):

    effective_date = datetime.strptime(
        dag_run.conf['terminationdate'], '%d-%b-%Y') + timedelta(days=1)

    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri']['uri']
        },
        "policySetScheduleEntries": rail.result('past_policyset_schedule') +
        [{
            "effectiveDate": effective_dateformat_payload(effective_date),
            "description": f"Effective on \
                {effective_date.month}/{effective_date.day}/{effective_date.year}",
            "policySet": {
                "timeOffBalanceEventScripts": [
                    {
                        "scriptTarget": {
                            "uri": dag_run.conf['scripttarget']
                        },
                        "additionalParameters": [
                            {
                                "keyUri": "urn:replicon:script-key:parameter:amount",
                                "value": {
                                    "number": 0
                                }
                            },
                            {
                                "keyUri": "urn:replicon:script-key:parameter:precedence",
                                "value": {
                                    "number": 20
                                }
                            }
                        ]
                    }
                ],
                "timeOffValidationScripts": []
            }
        }]
    }


def update_emp_daterange_hiredate(dag_run):
    if 'useruri' in dag_run.conf:
        user_uri = dag_run.conf['useruri']
    else:
        user_uri = rail.result('create_user')['uri']

    effective_date = datetime.strptime(dag_run.conf['hiredate'], '%d-%b-%Y')
    return {
        "userUri": user_uri,
        "dateRange": {
            "startDate": effective_dateformat_payload(effective_date)
        }
    }


def update_supervisorassignment_overdaterange(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "supervisorUri": rail.result('search_for_supervisor_with_managername')[0]['uri'],
        "dateRange": {
            "startDate": get_today_dateformat_payload()
        }
    }


def put_location_payload(dag_run):
    if rail.result('getenabled_location'):
        locationuri = rail.result('getenabled_location')
    else:
        locationuri = rail.result('publish_location')['uri']

    if 'useruri' in dag_run.conf:
        useruri = dag_run.conf['useruri']
    else:
        useruri = rail.result('create_user')['uri']
    return {
        "userUri": useruri,
        "scheduleEntries": [
            {
                "location": {
                    "uri": locationuri
                },
                "effectiveDate": get_today_dateformat_payload()
            }
        ]
    }


def put_division_payload(dag_run):
    if rail.result('getenabled_division'):
        divisionuri = rail.result('getenabled_division')
    else:
        divisionuri = rail.result('publish_division')['uri']

    if 'useruri' in dag_run.conf:
        useruri = dag_run.conf['useruri']
    else:
        useruri = rail.result('create_user')['uri']

    return {
        "userUri": useruri,
        "scheduleEntries": [
            {
                "division": {
                    "uri": divisionuri
                },
                "effectiveDate": get_today_dateformat_payload()
            }
        ]
    }


def userpayload_for_srvccntr(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "serviceCenterScheduleToApply": {
                "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "updateServiceCenterScheduleOverDateRange": {
                    "replacementServiceCenterScheduleEntries": [{
                        "serviceCenter": {
                            "uri": rail.result('get_enabled_servicecenters')['scheduledhour_srvcntr_uri']
                        },
                        "effectiveDate": get_today_dateformat_payload()
                    }]
                }
            }
        }
    }


def search_substituteuser_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:user-list-column:login-name"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf["substitutename"]
                }
            }
        }
    }


def get_createuser_payload(dag_run):
    departmenturi = "urn:replicon-tenant:" + \
        rail.get_tenant_slug() + ":department:1"
    return {
        "user": {
            "target": {
                "loginName": dag_run.conf['username']
            },
            "firstname": dag_run.conf['firstname'],
            "lastname": dag_run.conf['lastname'],
            "emailAddress": rail.result('log_usermail_id'),
            "employeeId": dag_run.conf['emplid'],
            "department": {
                "uri": departmenturi
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['username']
            },
            "permissionSets": [{
                "name": "Report User"
            }],
            "policySets": [{
                "name": "Time Off"
            }],
            "employeeType": {
                "name": rail.result('employeetype_to_assign')
            },
            "timesheetPeriodTypeUri": "urn:replicon:timesheet-period-type:system",
            "timesheetApprovalPath": {
                "name": "Supervisor"
            }
        }
    }


def puttimeoff_payload():
    return {
        'userUri': rail.result('create_user')['uri'],
        'timeOffTypeUris': rail.result('get_timeoffpolicyuri')
    }


def puttimeoffassignment_payload():
    return {
        'userUri': rail.result('create_user')['uri'],
        'timeOffTypeUris': rail.result('get_final_timeoff_list')
    }


def process_supervisor_mapper_data(item):
    return {
        "username": item['username'],
        "managername": item['managername'],
        "useruri": item['useruri'],
        "emplid": item['emplid'],
        "user_log": item['user_log']
    }


@lru_cache(maxsize=8)
def get_manager_data():
    return rail.result('load_distinct_managers_list')


def process_each_user_payload(item):
    manager_data = get_manager_data()
    return {
        'firstname': item['FirstName'],
        'lastname': item['LastName'],
        'username': item['UserName'],
        'workemail': item['WorkEmail'],
        'emplid': item['EmplID'],
        'employeetype': item['TimeType'],
        'hiredate': item['HireDate'],
        'termdate': item['TermDate'],
        'managername': item['ManagerName'],
        'department': item['Department'],
        'location': item['Location'],
        'substitutename': item['SubstituteName'],
        'timezone': item['Timezone'],
        'scheduledhours': item['ScheduledHours'],
        'managementlevel': item['ManagementLevel'],
        'division': item['Division'],
        'position': item['Position'],
        'homeworkstate': item['homeworkstate'],
        'employeestatus': item['employeestatus'],
        'approver': item['Approver'],
        'manager': manager_data.get(item['UserName'], "No"),
        'logger': rail.result('user_import_log'),
        'supervisor_logger': rail.result('supervisor_assignment_log'),
        'substitute_user_log': rail.result('create_substitute_user_log'),
        'employee_status_uri': rail.result('get_required_user_customfields')['employee_status_uri']
    }


def process_disable_user_payload(dag_run):
    return {
        'firstname': dag_run.conf['firstname'],
        'lastname': dag_run.conf['lastname'],
        'username': dag_run.conf['username'],
        'workemail': dag_run.conf['workemail'],
        'emplid': dag_run.conf['emplid'],
        'employeetype': dag_run.conf['employeetype'],
        'hiredate': dag_run.conf['hiredate'],
        'termdate': dag_run.conf['termdate'],
        'managername': dag_run.conf['managername'],
        'department': dag_run.conf['department'],
        'location': dag_run.conf['location'],
        'substitutename': dag_run.conf['substitutename'],
        'timezone': dag_run.conf['timezone'],
        'scheduledhours': dag_run.conf['scheduledhours'],
        'managementlevel': dag_run.conf['managementlevel'],
        'division': dag_run.conf['division'],
        'position': dag_run.conf['position'],
        'manager': dag_run.conf['manager'],
        'useruri': rail.result('get_user_data_based_on_emplid')[0]['userDetails']['uri'],
        'homeworkstate': dag_run.conf['homeworkstate'],
        'employeestatus': dag_run.conf['employeestatus'],
        'logger': rail.result('create_user_log'),
        'supervisor_logger': dag_run.conf['supervisor_logger'],
        'employee_status_uri': dag_run.conf['employee_status_uri']
    }


def process_update_user_payload(dag_run):
    return {
        'firstname': dag_run.conf['firstname'],
        'lastname': dag_run.conf['lastname'],
        'username': dag_run.conf['username'],
        'workemail': dag_run.conf['workemail'],
        'emplid': dag_run.conf['emplid'],
        'employeetype': dag_run.conf['employeetype'],
        'hiredate': dag_run.conf['hiredate'],
        'termdate': dag_run.conf['termdate'],
        'managername': dag_run.conf['managername'],
        'department': dag_run.conf['department'],
        'location': dag_run.conf['location'],
        'substitutename': dag_run.conf['substitutename'],
        'timezone': dag_run.conf['timezone'],
        'scheduledhours': dag_run.conf['scheduledhours'],
        'managementlevel': dag_run.conf['managementlevel'],
        'division': dag_run.conf['division'],
        'position': dag_run.conf['position'],
        'manager': dag_run.conf['manager'],
        'useruri': rail.result('get_user_data_based_on_emplid')[0]['userDetails']['uri'],
        'current_location': rail.result('get_current_groups_data')['current_location'],
        'current_division': rail.result('get_current_groups_data')['current_division'],
        'current_scheduledhour': rail.result('get_current_groups_data')['current_scheduledhour'],
        'homeworkstate': dag_run.conf['homeworkstate'],
        'employeestatus': dag_run.conf['employeestatus'],
        'approver': dag_run.conf['approver'],
        'logger': rail.result('create_user_log'),
        'supervisor_logger': dag_run.conf['supervisor_logger'],
        'substitute_user_log': dag_run.conf['substitute_user_log']
    }


def process_add_user_payload(dag_run):
    return {
        'firstname': dag_run.conf['firstname'],
        'lastname': dag_run.conf['lastname'],
        'username': dag_run.conf['username'],
        'workemail': dag_run.conf['workemail'],
        'emplid': dag_run.conf['emplid'],
        'employeetype': dag_run.conf['employeetype'],
        'hiredate': dag_run.conf['hiredate'],
        'termdate': dag_run.conf['termdate'],
        'managername': dag_run.conf['managername'],
        'department': dag_run.conf['department'],
        'location': dag_run.conf['location'],
        'substitutename': dag_run.conf['substitutename'],
        'timezone': dag_run.conf['timezone'],
        'scheduledhours': dag_run.conf['scheduledhours'],
        'managementlevel': dag_run.conf['managementlevel'],
        'division': dag_run.conf['division'],
        'position': dag_run.conf['position'],
        'manager': dag_run.conf['manager'],
        'homeworkstate': dag_run.conf['homeworkstate'],
        'employeestatus': dag_run.conf['employeestatus'],
        'approver': dag_run.conf['approver'],
        'logger': rail.result('create_user_log'),
        'supervisor_logger': dag_run.conf['supervisor_logger'],
        'substitute_user_log': dag_run.conf['substitute_user_log']
    }
