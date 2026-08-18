import hashlib
import uuid
import ast
import json
from datetime import datetime
import re
import rail
from airflow.models import Variable

null = None


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_create_md5_data(item):
    if not item:
        return []
    res = {
        **dict(item.items()),
        **{
            'md5': hashlib.md5((item["positionid"]+","+item["firstname"]+","+item["lastname"]+","
                                + item["flsadescription"]+"," + item["internationalflsa"] +
                                "," + item["workercategorydescription"]
                                + item["positionstatus"]+"," + item["hiredate"] +
                                "," + item["rehiredate"]
                                + item["terminationdate"]+"," +
                                item["reportstoid"]+"," + item["reportstoname"]
                                + item["workemail"]+"," + item["locationcode"] +
                                "," + item["locationdescription"]
                                + item["homecostnumbercode"]+"," +
                                item["homecostnumberdescription"] +
                                "," + item["unionlocalcode"]
                                + item["unionlocaldescription"]+"," +
                                item["businessunitcode"]+"," +
                                item["businessunitdescription"]
                                + item["regularpayrateamount"]+"," +
                                item["rate2"]
                                ).encode('utf-8')).hexdigest()
        }
    }

    return {k: v if v is not None else '' for k, v in res.items()}


def create_departments_in_replicon(item):
    return {
        "division": null,
        "modifications": {
            "name": item['businessunitdescription'],
            "codeToApply": {
                "value": item['businessunitcode']
            },
            "descriptionToApply": null,
            "isEnabled": "true"
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def create_cost_center_in_replicon(item):
    return {
        "hierarchy": [
            {
                "target": null,
                "parameterCorrelationId": null,
                "modificationToApply": {
                    "name": item["CostName"],
                    "codeToApply": {
                        "value": item["CostCode"]
                    },
                    "descriptionToApply": null,
                    "isEnabled": null
                }
            }
        ],
        "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def create_locations_in_replicon(item):
    return {
        "hierarchy": [
            {
                "target": null,
                "parameterCorrelationId": null,
                "modificationToApply": {
                    "name": item['locationdescription'],
                    "codeToApply": {
                        "value": item['locationcode']
                    },
                    "descriptionToApply": null,
                    "isEnabled": True
                }
            }
        ],
        "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_process_each_record_conf(item, config):

    payrule_sync_mapper = ast.literal_eval(
        Variable.get(config.payrule_sync_mapper))

    def get_employee_type():
        if item['flsadescription']:
            if item['workercategorydescription'] in ["Temporary", "Intern"]:
                return item['workercategorydescription'] + " " + item['flsadescription']
            return item['flsadescription'] + " " + item['workercategorydescription']
        if item['workercategorydescription'] in ["Temporary", "Intern"]:
            return item['workercategorydescription'] + " " + item['internationalflsa']
        return item['internationalflsa'] + " " + item['workercategorydescription']

    def get_timesheet_template_name():

        employee_type = get_employee_type()
        if "Non-exempt" in employee_type:
            return "Non-Exempt Standard Template"
        return "Exempt Standard Template"

    def get_payrule_script_name():
        return rail.find_first_by_attr_and_get_attr(payrule_sync_mapper, 'Location', item['locationdescription'], 'Payrule') \
            if "Non-exempt" in get_employee_type() else "Exempt Payrule"

    locationuri = rail.find_first_by_attr_and_get_attr(rail.result(
        'get_all_locations'), 'displayText', item['locationdescription'], 'uri')

    timesheettemplate = get_timesheet_template_name()

    timesheettemplateuri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_policy_sets'), 'displayText', timesheettemplate, 'uri')

    payrulescripturi = rail.find_first_by_attr_and_get_attr(rail.result(
        'get_all_payrule_scripts'), 'displayText', get_payrule_script_name(), 'uri')

    def timezone_name():
        return rail.find_first_by_attr_and_get_attr(payrule_sync_mapper, 'Location', item['locationdescription'], 'TimeZone')

    def get_timesheet_period():
        if item['positionid'][:3] == "5HK":
            return "Biweekly starting on Monday"
        return "Semimonthly"

    def get_holiday_calendar_name():
        return rail.find_first_by_attr_and_get_attr(payrule_sync_mapper, 'Location', item['locationdescription'], 'Holiday Calendar')

    return {
        **dict(item.items()),
        **{
            'loginname': item['workemail'],
            'employeeid': item['positionid'],
            'enduseruri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'), 'displayText',
                                                               'Project Resource with Reports', 'uri'),
            'timesheetapproveruri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'), 'displayText', 'Supervisor', 'uri'),
            'timezone': timezone_name(),
            'timezoneuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_timezones'), 'displayText', timezone_name(), 'uri'),
            'departmenturi': rail.find_first_by_attr_and_get_attr(rail.result('get_all_departments'), 'displayText',
                                                                  "Avenu US" if item['positionid'][:3] == "5HK" else "Avenu International", 'uri'),
            'locationuri': locationuri,
            'timesheettemplate': timesheettemplate,
            'timesheettemplateuri': timesheettemplateuri,
            'workweekuri': config.work_week_uri,
            'payrulescript': get_payrule_script_name(),
            'payrulescripturi': payrulescripturi,
            'timeofftemplate': config.timeofftemplate,
            'timesheet_period_schedule': get_timesheet_period(),
            'schedule_policy': config.schedule_policy,
            'holidaycalender': get_holiday_calendar_name(),
            'holidaycalenderuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calenders'), 'displayText',
                                                                       get_holiday_calendar_name(), 'uri'),
            'employee_type': get_employee_type(),
            'employeetypeuri':  rail.find_first_by_attr_and_get_attr(rail.result('get_all_employeetypes'), 'code', get_employee_type(), 'uri'),
            'california_punch_policy': rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'), 'displayText',
                                                                            'California Avenu Punch Policy', 'uri'),
            'avenu_punch_policy': rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'), 'displayText', 'Avenu Punch Policy', 'uri'),
            'dummy_punch_policy': rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'), 'displayText', 'Dummy Punch Entry Policy', 'uri'),
            'dummy_ca_policy': rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'), 'displayText', 'Dummy CA Punch Entry Policy', 'uri'),
            'cnr_weekly_rule_uri' :rail.result('get_weekly_rule_cost_normalization_uri')
        }
    }


def get_user_data_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
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
                    "text": dag_run.conf['employeeid'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_today_date():
    now = datetime.now()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_process_user_conf(dag_run, status):
    return {
        **dict(dag_run.conf),
        **{
            'useruri': rail.result('get_user_data')[0]['uri'] if status == 'update_user'else null,
            'status': rail.result('get_user_data')[0]['status'] if status == 'update_user'else null,
            'todays_date': get_today_date()
        }
    }


def get_replicon_date(date_str):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, '%m/%d/%Y')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def test_valid_fields(dag_run):
    email_regex = re.compile(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    flag = True
    if dag_run.conf['hiredate']:
        startdate = get_replicon_date(dag_run.conf['hiredate'])
        if not startdate:
            flag = False
    if dag_run.conf['rehiredate']:
        rehiredate = get_replicon_date(dag_run.conf['rehiredate'])
        if not rehiredate:
            flag = False
    if dag_run.conf['terminationdate']:
        enddate = get_replicon_date(dag_run.conf['terminationdate'])
        if not enddate:
            flag = False
    if dag_run.conf['positionstatus'] not in ["Leave", "Deceased", "Retired", "Terminated", "Active"]:
        flag = False
    if dag_run.conf['employee_type'] not in ["Non-exempt Part Time", "Non-exempt Full Time",
                                             "Temporary Non-exempt", "Intern Non-exempt", "Exempt Full Time",
                                             "Exempt Part Time", "Temporary Exempt", "Intern Exempt"]:
        flag = False
    else:
        if test_non_exempt_employee_type(dag_run):
            if not dag_run.conf['regularpayrateamount']:
                flag = False
        else:
            if not dag_run.conf['rate2']:
                flag = False
    if dag_run.conf['workemail'] and not re.fullmatch(email_regex, dag_run.conf['workemail']):
        flag = False
    return flag


def test_non_exempt_employee_type(dag_run):
    if "Non-exempt" in dag_run.conf["employee_type"]:
        return True
    return False

def test_exempt_employee_type(dag_run):
    if "Non-exempt" in dag_run.conf["employee_type"]:
        return False
    return True


def user_position_status_check(dag_run):

    if dag_run.conf['positionstatus'] in ["Leave", "Deceased", "Retired", "Terminated"]:
        return True
    return False


def test_status_delete(dag_run):

    if dag_run.conf['positionstatus'] in ["Deceased", "Retired", "Terminated"]:
        return True
    return False


def get_invalid_fields_message(dag_run):
    email_regex = re.compile(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    log = []
    if dag_run.conf['hiredate']:
        startdate = get_replicon_date(dag_run.conf['hiredate'])
        if not startdate:
            log.append('Invalid format for Start Date')
    if dag_run.conf['rehiredate']:
        rehiredate = get_replicon_date(dag_run.conf['rehiredate'])
        if not rehiredate:
            log.append('Invalid format for Rehire Date')
    if dag_run.conf['terminationdate']:
        enddate = get_replicon_date(dag_run.conf['terminationdate'])
        if not enddate:
            log.append('Invalid format for Termination Date')
    if dag_run.conf['positionstatus'] not in ["Leave", "Deceased", "Retired", "Terminated", "Active"]:
        log.append('Position Status is not valid')
    if dag_run.conf['employee_type'] not in ["Non-exempt Part Time", "Non-exempt Full Time",
                                             "Temporary Non-exempt", "Intern Non-exempt", "Exempt Full Time",
                                             "Exempt Part Time", "Temporary Exempt", "Intern Exempt"]:
        log.append('Employee Type is not valid')
    else:
        if test_non_exempt_employee_type(dag_run):
            if not dag_run.conf['regularpayrateamount']:
                log.append('Regular Payrate is Empty for Non-Exempt')
        else:
            if not dag_run.conf['rate2']:
                log.append('Rate 2 is Empty for Exempt')
    if dag_run.conf['workemail'] and not re.fullmatch(email_regex, dag_run.conf['workemail']):
        log.append('Email Id is Inccorrect')

    return str(log)[1:-1]


def get_put_user_payload(dag_run):

    currency = ":currency:1" if dag_run.conf['positionid'][:3] == "5HK" else ":currency:2"

    def get_value():
        if test_non_exempt_employee_type(dag_run):
            return "regularpayrateamount"
        return "rate2"

    def update_enddate():
        enddate = datetime.strptime(
            str(dag_run.conf['terminationdate']), "%m/%d/%Y")
        startdate = datetime.strptime(
            str(dag_run.conf['hiredate']), "%m/%d/%Y")
        return bool(enddate > startdate)

    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['loginname'],
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf['firstname'],
            "lastname": dag_run.conf['lastname'],
            "emailAddress": dag_run.conf['workemail'],
            "employeeId": dag_run.conf['positionid'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": null,
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf['schedule_policy']
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate":get_replicon_date(dag_run.conf['hiredate'])
                }
            ],
            "workWeekStartDayUri": dag_run.conf['workweekuri'],
            "employmentDateRange": {
                "startDate": get_replicon_date(dag_run.conf['hiredate']),
                "endDate": get_replicon_date(dag_run.conf['terminationdate']) if dag_run.conf['terminationdate'] and update_enddate() else null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['loginname'],
                "SSOName": dag_run.conf['loginname'],
                "password": null
            },
            "holidayCalendar": null if not dag_run.conf['holidaycalender'] else {
                "uri": dag_run.conf['holidaycalenderuri'],
                "name": null
            },
            "timeOffPolicy": null,
            "permissionSets": [
                {
                    "uri": dag_run.conf['enduseruri'],
                    "name": null
                }
            ],
            "policySets":  [
                {
                    "uri": null,
                    "name": dag_run.conf['timeofftemplate']
                }
            ] if not dag_run.conf['timesheettemplateuri'] else
            [
                {
                    "uri": dag_run.conf['timesheettemplateuri'],
                    "name": null
                },
                {
                    "uri": null,
                    "name": dag_run.conf['timeofftemplate']
                }
            ],
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            # check this
            "costRateSchedule": {
                "initialHourlyRate": {
                    "amount": float(dag_run.conf[get_value()].replace(",", ""))*1.2,
                    "currency": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+currency,
                        "name": null,
                        "symbol": null
                    }
                },
                "scheduleEntries": [
                    {
                    "hourlyRate": {
                        "amount": float(dag_run.conf[get_value()].replace(",", ""))*1.2,
                        "currency": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+currency,
                        }
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['hiredate'])
                    }
                ]
            },
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": null,
            "expenseApprovalPath": null,
            "timeOffApprovalPath": null,
            "customFieldValues": [],
            "assignedActivities": [],
            "timeZone": {
                "uri": dag_run.conf['timezoneuri'],
                "IANAName": null
            },
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [
                {
                    "location": {
                        "uri": dag_run.conf['locationuri'],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['hiredate'])
                }
            ],
            "divisionSchedule": [{
                "division": {
                    "uri": null,
                    "parentUri": null,
                    "name": dag_run.conf['businessunitdescription']
                },
                "effectiveDate": get_replicon_date(dag_run.conf['hiredate'])
            }],
            "costCenterSchedule": [{
                "costCenter": {
                    "uri": null,
                    "parentUri": null,
                    "name": dag_run.conf["homecostnumberdescription"] if dag_run.conf['homecostnumbercode'] else dag_run.conf["unionlocaldescription"]
                },
                "effectiveDate": get_replicon_date(dag_run.conf['hiredate'])
            }],
            "serviceCenterSchedule": [],
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "uri": dag_run.conf['departmenturi'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['hiredate'])
                }
            ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": dag_run.conf['employeetypeuri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['hiredate'])
                }
            ],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": dag_run.conf['timesheet_period_schedule']
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['hiredate'])
                }
            ],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [
                {
                    "payRuleScript": {
                        "uri": dag_run.conf['payrulescripturi'],
                        "name": null
                    },
                    "effectiveDate": get_replicon_date(dag_run.conf['hiredate'])
                }
            ],
            "displayNameParameter": null,
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": null
        }
    }


def get_all_employee_grp_payload():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:employee-type-group",
            "urn:replicon:employee-type-group-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:employee-type-group-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "bool": "true",
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_remove_timeoff_payload():
    return {
        "userUri": rail.result('add_new_user')['uri'],
        "timeOffTypeUris": []
    }


def get_data_for_supervisor_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:end-date"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['reportstoid']
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def is_full_time_employee_type(dag_run):
    return "full time" in dag_run.conf['employee_type'].lower()

def get_process_time_off_assignment_conf(dag_run, status):
    return {
        **dict(dag_run.conf),
        **{
            'usertype': status,
            'useruritimeoff': rail.result('add_new_user')['uri'] if status == 'new_user' else dag_run.conf['useruri'],
            'log_exception': rail.result('add_user_exception_log') if status == 'new_user' else rail.result('update_user_exception_log'),
            'log_error': rail.result('add_user_error_logs') if status == 'new_user' else rail.result('update_user_error_logs'),
            'time_off_only': "" if status == 'new_user' else list(filter(lambda name: name['customField']['displayText'] == "Time Off Only",
                                                                         rail.result('get_user_info')['userDetails']['customFieldValues']))[0]['text'],
            'is_rehire_user': "" if status == 'new_user' else
            ("Y" if not rail.result('get_user_info')
             ['userDetails']['isEnabled'] else "N"),
            "is_employee_transerfred":  "No" if status == "new_user" else ( "Yes" if is_employee_type_changed(dag_run) else "No"),
            "is_full_time_employee_type": "NO" if status == "new_user" else ( "Yes" if is_full_time_employee_type(dag_run) else "No"),
            "user_profile_end_date": bool(rail.result('get_user_info').get("userDetails",{}).get("employmentDateRange",{}).get("endDate", None)
                                          if rail.result('get_user_info') else None)
        }
    }


def check_rehire_time_off_scenario(dag_run):
    if dag_run.conf["is_rehire_user"] == "Y":
        return True
    return False


def get_update_time_off_for_no_aacural(dag_run, item):
    return {
        **dict(dag_run.conf),
        **{
            'policyset': json.dumps(ast.literal_eval(str(item["policySetSchedule"]).replace("[[{", "[{").replace("}]]", "}]"))),
            'timeoffuriaccural': item['timeOffType']['uri'],
            'is_employee_transerfred': "No",
            "is_full_time_employee_type": "No"
        }
    }


def get_delete_time_off_scenario(dag_run):
    return {
        **dict(dag_run.conf),
        **{
            "is_employee_transerfred": "No",
            "is_full_time_employee_type": "No"
        }
    }


def get_delete_future_time_off_scenario(dag_run, item):
    return {
        **dict(dag_run.conf),
        **{
            'timeofftype': item
        }
    }


def test_timeoff_availablity():
    data = rail.result('get_timeoff_type_list')
    if data[0]['timeofftypename'] == "NA":
        return True
    check = rail.find_first_by_attr_and_get_attr(
        data, 'timeofftypeuri', 'Not Available', 'timeofftypeuri')
    return bool(check != 'Not Available')


def put_timeoff_assignment_for_user(dag_run):
    return {
        "userUri": dag_run.conf['useruritimeoff'],
        "timeOffTypeUris": [] if rail.result('get_time_off_type_uris')[0] == "NA" else rail.result('get_time_off_type_uris')
    }


def get_process_time_off_policy_new_user_conf(item, dag_run):
    return {
        **dict(dag_run.conf),
        **{
            'timeofftypename': item['timeofftypename'],
            'timeofftypeuri': item['timeofftypeuri']
        }
    }


def get_default_timeoff_policy_schedule_payload(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruritimeoff'],
            "timeOffTypeUri": dag_run.conf['timeofftypeuri']
        }
    }


def get_user_timeoff_policy_payload(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruritimeoff'],
            "timeOffTypeUri": dag_run.conf['timeofftypeuri']
        },
        "policySetScheduleEntries": json.loads(rail.result('policy_to_assign'))
    }


def check_enddate(dag_run):
    if dag_run.conf['terminationdate']:
        enddate = datetime.strptime(
            str(dag_run.conf['terminationdate']), "%m/%d/%Y")
        startdate = datetime.strptime(
            str(dag_run.conf['hiredate']), "%m/%d/%Y")
        return 'Enddate is prior to Startdate' if enddate < startdate else ''
    return ''


def get_add_completion_message(dag_run):
    return ('User Added Partially, ' + str(rail.result('get_all_error_logs'))[1:-1])\
        if rail.result('get_all_error_logs') else (('User Added Partially, ' + str(rail.result('get_all_exception_logs'))[1:-1] + check_enddate(dag_run))
                                                   if rail.result('get_all_exception_logs') or check_enddate(dag_run) != '' else 'User Added Successfully')


def get_add_severity(dag_run):
    supervisor_status = get_task_state(
        'log_supervisor_not_present') == 'success'
    return 'Pending_User' if supervisor_status else ('Error' if rail.result('get_all_error_logs') else (
        'Exception' if rail.result('get_all_exception_logs') or check_enddate(dag_run) != '' else 'Add_Success'))


def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])


def apply_user_modifications(dag_run):
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-branches

    def update_timezone():
        assigned_timezone = rail.result('get_user_info')['timeZone']
        if not assigned_timezone:
            if dag_run.conf['timezone']:
                return {
                    "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                    "timezone": {
                        "uri": dag_run.conf['timezoneuri'],
                        "IANAName": null
                    }
                }
            return null
        if dag_run.conf['timezone'] and (dag_run.conf['timezone'] != rail.result('get_user_info')['timeZone']['displayText']):
            return {
                "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": {
                    "uri": dag_run.conf['timezoneuri'],
                    "IANAName": null
                }
            }
        return null

    def update_holiday_calender():
        assigned_holiday_calender = rail.result(
            'get_user_info')['holidayCalendar']
        if not assigned_holiday_calender:
            if dag_run.conf['holidaycalender'] and dag_run.conf['holidaycalenderuri']:
                return {
                    "holidayCalendar": {
                        "uri": dag_run.conf['holidaycalenderuri'],
                        "name": null
                    }
                }
            return null
        if (dag_run.conf['holidaycalender'] and dag_run.conf['holidaycalenderuri']) and\
                (dag_run.conf['holidaycalender'] != rail.result('get_user_info')['holidayCalendar']['displayText']):
            return {
                "holidayCalendar": {
                    "uri": dag_run.conf['holidaycalenderuri'],
                    "name": null
                }
            }
        return null

    def update_payrule_script():
        assigned_payrules = rail.result("get_user_info")[
            'payRuleScriptSchedule']
        if not assigned_payrules:
            if dag_run.conf['payrulescripturi']:
                return {
                    "scheduleEntries": [
                        {
                            "payRuleScript": {
                                "uri": dag_run.conf['payrulescripturi'],
                                "name": null
                            },
                            "effectiveDate": dag_run.conf['todays_date']
                        }
                    ]
                }
            return null
        if (dag_run.conf['payrulescript'] and dag_run.conf['payrulescripturi']) \
                and (dag_run.conf['payrulescript'] != assigned_payrules[-1]['payRuleScript']['displayText']):
            return {
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": dag_run.conf['payrulescripturi'],
                            "name": null
                        },
                        "effectiveDate": dag_run.conf['todays_date']
                    }
                ]
            }
        return null

    def update_permission_set():
        if rail.find_first_by_attr_and_get_attr(rail.result('get_user_info')['permissionSets'],
                                                'displayText', 'Project Resource with Reports', 'displayText') != 'Project Resource with Reports':
            return {
                "permissionSetUrisToAssign": [
                    dag_run.conf['enduseruri']
                ],
                "policyUrisToRemovePermissionSet": []
            }
        return null

    def update_department_grp():
        assigned_department_grp = rail.result(
            "get_user_info")['departmentGroupSchedule']

        if not assigned_department_grp:
            return {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": {
                                "uri": dag_run.conf['departmenturi'],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": dag_run.conf['todays_date']
                        }
                    ],
                    "endDate": null
                }
            }

        value = "Avenu US" if dag_run.conf['positionid'][:
                                                         3] == "5HK" else "Avenu International"
        if value != assigned_department_grp[-1]['departmentGroup']['displayText']:
            return {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": {
                                "uri": dag_run.conf['departmenturi'],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": dag_run.conf['todays_date']
                        }
                    ],
                    "endDate": null
                }
            }
        return null

    def update_cost_center_grp():
        assigned_department_grp = rail.result(
            "get_user_info")['costCenterSchedule']

        value_to_assign = dag_run.conf['homecostnumberdescription'] if dag_run.conf[
            'homecostnumberdescription'] else dag_run.conf['unionlocaldescription']

        if not assigned_department_grp:
            if dag_run.conf['homecostnumberdescription'] or dag_run.conf['unionlocaldescription']:
                return {
                    "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementCostCenterSchedule": [],
                    "updateCostCenterScheduleOverDateRange": {
                        "replacementCostCenterScheduleEntries": [
                            {
                                "costCenter": {
                                    "uri": null,
                                    "parentUri": null,
                                    "name": value_to_assign
                                },
                                "effectiveDate": null
                            }
                        ],
                        "endDate": null
                    }
                }
            return null

        if (dag_run.conf['homecostnumberdescription'] or dag_run.conf['unionlocaldescription']) and \
                value_to_assign != assigned_department_grp[-1]['costCenter']['displayText']:
            return {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementCostCenterSchedule": [],
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {
                                "uri": null,
                                "parentUri": null,
                                "name": value_to_assign
                            },
                            "effectiveDate": dag_run.conf['todays_date']
                        }
                    ],
                    "endDate": null
                }
            }
        return null

    def update_employee_type_grp():
        assigned_employee_type_grp = rail.result(
            "get_user_info")['employeeTypeGroupSchedule']
        if not assigned_employee_type_grp:
            if dag_run.conf['employee_type']:
                return {
                    "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementEmployeeTypeGroupSchedule": [],
                    "updateEmployeeTypeGroupScheduleOverDateRange": {
                        "replacementEmployeeTypeGroupScheduleEntries": [
                            {
                                "employeeTypeGroup": {
                                    "uri": dag_run.conf['employeetypeuri'],
                                    "parent": null,
                                    "name": null,
                                    "parameterCorrelationId": null
                                },
                                "effectiveDate": dag_run.conf['todays_date']
                            }
                        ],
                        "endDate": null
                    }
                }
            return null

        if dag_run.conf['employee_type'] and \
                dag_run.conf['employee_type'] != assigned_employee_type_grp[-1]['employeeTypeGroup']['displayText']:
            return {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {
                                "uri": dag_run.conf['employeetypeuri'],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": dag_run.conf['todays_date']
                        }
                    ],
                    "endDate": null
                }
            }
        return null

    def update_location_grp():
        assigned_locations = rail.result("get_user_info")['locationSchedule']
        if not assigned_locations:
            if dag_run.conf['locationuri']:
                return {
                    "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementLocationSchedule": [],
                    "updateLocationScheduleOverDateRange": {
                        "replacementLocationScheduleEntries": [
                            {
                                "location": {
                                    "uri": dag_run.conf['locationuri'],
                                    "parentUri": null,
                                    "name": null
                                },
                                "effectiveDate": dag_run.conf['todays_date']
                            }
                        ],
                        "endDate": null
                    }
                }
            return null
        if dag_run.conf['locationuri'] and dag_run.conf['locationdescription'] != assigned_locations[-1]['location']['displayText']:
            return {
                "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementLocationSchedule": [],
                "updateLocationScheduleOverDateRange": {
                    "replacementLocationScheduleEntries": [
                        {
                            "location": {
                                "uri": dag_run.conf['locationuri'],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": dag_run.conf['todays_date']
                        }
                    ],
                    "endDate": null
                }
            }
        return null

    def update_policy_set():
        assigned_timesheet_template = rail.result(
            "get_user_info")['timesheetTemplate']

        time_off_only = list(filter(lambda name: name['customField']['displayText'] == "Time Off Only", rail.result(
            'get_user_info')['userDetails']['customFieldValues']))[0]['text']
        if time_off_only == "Yes":
            return null
        if not assigned_timesheet_template:
            if dag_run.conf['timesheettemplateuri']:
                return {
                    "policySetUrisToAssign": [
                        dag_run.conf['timesheettemplateuri']
                    ],
                    "policyUrisToRemovePolicySet": []
                }
            return null

        if (dag_run.conf['timesheettemplate'] and dag_run.conf['timesheettemplateuri']) \
                and (dag_run.conf['timesheettemplate'] != assigned_timesheet_template['displayText']):
            return {
                "policySetUrisToAssign": [
                    dag_run.conf['timesheettemplateuri']
                ],
                "policyUrisToRemovePolicySet": []
            }
        return null

    def get_value():
        if test_non_exempt_employee_type(dag_run):
            return "regularpayrateamount"
        return "rate2"

    def update_cost_rate():
        if test_hourly_rate(dag_run):
            currency = ":currency:1" if dag_run.conf['positionid'][:3] == "5HK" else ":currency:2"
            return {
                "scheduleEntriesToAdd": [
                    {
                        "hourlyRate": {
                            "amount": float(dag_run.conf[get_value()].replace(",", ""))*1.2,
                            "currency": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+currency,
                                "name": null,
                                "symbol": null
                            }
                        },
                        "effectiveDate": dag_run.conf['todays_date']
                    }
                ],
                "scheduleEntriesToPut": null
            }
        return null

    def update_division_group():
        assigned_division_group = rail.result(
            "get_user_info")['divisionSchedule']

        if not assigned_division_group:
            return {
                "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDivisionSchedule": [],
                "updateDivisionScheduleOverDateRange": {
                    "replacementDivisionScheduleEntries": [
                        {
                            "division": {
                                "uri": null,
                                "parentUri": null,
                                "name": dag_run.conf['businessunitdescription']
                            },
                            "effectiveDate": null
                        }
                    ],
                    "endDate": null
                }
            }
        if dag_run.conf['businessunitdescription'] != assigned_division_group[-1]['division']['displayText']:
            return {
                "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDivisionSchedule": [],
                "updateDivisionScheduleOverDateRange": {
                    "replacementDivisionScheduleEntries": [
                        {
                            "division": {
                                "uri": null,
                                "parentUri": null,
                                "name": dag_run.conf['businessunitdescription']
                            },
                            "effectiveDate": dag_run.conf['todays_date']
                        }
                    ],
                    "endDate": null
                }
            }
        return null

    def is_rehire_user():
        return not rail.result('get_user_info')['userDetails']['isEnabled']

    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timezoneToApply": update_timezone(),
            "holidayCalendarToApply": update_holiday_calender(),
            "locationScheduleToApply": update_location_grp(),
            "divisionScheduleToApply": update_division_group(),
            "costCenterScheduleToApply": update_cost_center_grp(),
            "departmentGroupScheduleToApply": update_department_grp(),
            "employeeTypeGroupScheduleToApply": update_employee_type_grp(),
            "permissionSetsToApply": update_permission_set(),
            'policySetsToApply': update_policy_set(),
            "payRulesScheduleModifications": update_payrule_script(),
            "costRateScheduleModifications": update_cost_rate(),
            "userDetailsToApply": {
                "firstName": dag_run.conf['firstname'],
                "lastName": dag_run.conf['lastname'],
                "emailAddress": {
                    "emailAddress": dag_run.conf['workemail']
                },
                "language": null,
                "employmentDateRange": null,
                "employmentStartDate": {
                    "date": get_replicon_date(dag_run.conf['rehiredate'] if dag_run.conf['rehiredate'] else dag_run.conf['hiredate']) if is_rehire_user()
                    else rail.result('get_user_info')['userDetails']['employmentDateRange']['startDate']
                },
                "employmentEndDate": {
                    "date": get_replicon_date(dag_run.conf['terminationdate']) if dag_run.conf['terminationdate'] else null
                },
                "employeeId": null,
                "displayNameParameter": null
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def test_hourly_rate(dag_run):
    assigned_hourly_rate = rail.result('get_user_info')['costRateSchedule']
    if not assigned_hourly_rate:
        return True
    if dag_run.conf['positionid'][:3] == "5HK":
        if dag_run.conf['flsadescription'] == "Exempt":
            if float(dag_run.conf['rate2'].replace(",", "")) != float(assigned_hourly_rate[-1]['hourlyRate']['amount']):
                return True
        else:
            if float(dag_run.conf['regularpayrateamount'].replace(",", "")) != float(assigned_hourly_rate[-1]['hourlyRate']['amount']):
                return True
    else:
        if dag_run.conf['internationalflsa'] == "Exempt":
            if float(dag_run.conf['rate2'].replace(",", "")) != float(assigned_hourly_rate[-1]['hourlyRate']['amount']):
                return True
        else:
            if float(dag_run.conf['regularpayrateamount'].replace(",", "")) != float(assigned_hourly_rate[-1]['hourlyRate']['amount']):
                return True
    return False


def update_hourly_rate(dag_run):
    def get_value():
        if test_non_exempt_employee_type(dag_run):
            return "regularpayrateamount"
        return "rate2"

    currency = ":currency:1" if dag_run.conf['positionid'][:3] == "5HK" else ":currency:2"
    return {
        "userUri": dag_run.conf['useruri'],
        "hourlyRate": {
            "amount": dag_run.conf[get_value()].replace(",", ""),
            "currencyUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+currency
        },
        "dateRange": {
            "startDate": dag_run.conf['todays_date'],
            "endDate": null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def get_assigned_policy_to_user():
    return {
        "userUri": rail.result('get_user_info')['userDetails']["uri"]
    }


def add_hourly_rate(dag_run):
    def get_value():
        if test_non_exempt_employee_type(dag_run):
            return "regularpayrateamount"
        return "rate2"

    currency = ":currency:1" if dag_run.conf['positionid'][:3] == "5HK" else ":currency:2"
    return {
        "userUri": rail.result("add_new_user")["uri"],
        "hourlyRate": {
            "amount": dag_run.conf[get_value()].replace(",", ""),
            "currencyUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+currency
        },
        "dateRange": {
            "startDate": get_replicon_date(dag_run.conf['hiredate']),
            "endDate": null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def get_file_id_uri():
    return {
        "objectUri": rail.result("add_new_user")["uri"]
    }


def add_file_id(dag_run):
    return {
        "objectUri": rail.result("add_new_user")["uri"],
        "customFieldUri": rail.result("get_file_id_uri"),
        "value": dag_run.conf['positionid'][-6:]
    }


def assign_punch_entry_policy(dag_run, config):
    return {
        "userUri": rail.result("add_new_user")["uri"],
        "policySetUri": dag_run.conf["california_punch_policy"] if dag_run.conf["locationdescription"] in config.ca_locations
        else dag_run.conf["avenu_punch_policy"]
    }


def update_punch_entry_policy(dag_run, config):
    current_employee_type = rail.result("get_user_info")[
        "employeeTypeGroupSchedule"][-1]["employeeTypeGroup"]["displayText"]
    file_employee_type = dag_run.conf["employee_type"]

    final_policy = ""

    if "Non-exempt" in current_employee_type:
        if "Exempt" in file_employee_type:
            current_policy = rail.result("get_assigned_policy_to_user")[
                0]["policySet"]["displayText"] if rail.result("get_assigned_policy_to_user") else ""
            if current_policy:
                if current_policy == "Avenu Punch Policy":
                    final_policy = dag_run.conf["dummy_punch_policy"]
                else:
                    final_policy = dag_run.conf["dummy_ca_policy"]
    if not final_policy:
        final_policy = dag_run.conf["california_punch_policy"] if dag_run.conf[
            "locationdescription"] in config.ca_locations else dag_run.conf["avenu_punch_policy"]

    return {
        "userUri": rail.result('get_user_info')['userDetails']["uri"],
        "policySetUri": final_policy
    }


def get_update_completion_message(dag_run):
    return ('User Updated Partially, ' + str(rail.result('get_all_error_logs'))[1:-1] + str(rail.result('get_all_success_logs'))[1:-1])\
        if rail.result('get_all_error_logs') else (('User Updated Partially, ' + str(rail.result('get_all_exception_logs'))[1:-1] +
                                                    str(rail.result('get_all_success_logs'))[1:-1] + check_enddate(dag_run))
                                                   if rail.result('get_all_exception_logs') or check_enddate(dag_run) != ''
                                                   else ('User Updated Successfully' + str(rail.result('get_all_success_logs'))[1:-1]))


def get_update_severity(dag_run):
    supervisor_status = get_task_state(
        'log_supervisor_not_present') == 'success'
    return 'Pending_User' if supervisor_status else ('Error' if rail.result('get_all_error_logs') else (
        'Exception' if rail.result('get_all_exception_logs') or check_enddate(dag_run) != '' else 'Update_Success'))


def get_supervisor_conf(item):
    return {
        **dict(item['properties'].items()),
        'enduseruri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'), 'displayText', 'Project Resource with Reports', 'uri'),
        'timesheetapproveruri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'), 'displayText', 'Supervisor', 'uri'),
    }


def get_user_time_off_policy_summary(dag_run):
    return {
        "userUri": dag_run.conf['useruri']
    }


def get_process_time_off_policy_update_rehire_user(item, dag_run):

    return {
        **dict(dag_run.conf),
        **{
            'timeofftypename': item['timeofftypename'],
            'timeofftypeuri': item['timeofftypeuri']
        }
    }


def get_process_time_off_policy_rehire_user(item, dag_run):

    return {
        **dict(dag_run.conf),
        **{
            'timeofftypename': item['name'],
            'timeofftypeuri': item['uri']
        }
    }


def get_supervisor_check_severity(dag_run):
    previous_status = rail.result('get_message_from_log')[0]['status']

    def check_status():
        if previous_status == 'Success':
            if dag_run.conf['action'] == 'Add':
                return 'Add_Success'
            return 'Update_Success'
        return previous_status

    return 'Exception' if ((get_task_state('log_supervisor_not_present') == 'success'
                            or get_task_state('log_supervisor_end_date_in_past') == 'success') and previous_status != 'Error') else check_status()


def get_supervisor_message_master_log():
    previous_message = rail.result('get_message_from_log')[0]['message']
    previous_status = rail.result('get_message_from_log')[0]['status']

    def get_message():
        log = ''
        if get_task_state('log_supervisor_not_present') == 'success':
            log = 'Supervisor not present in replicon'
        if get_task_state('log_supervisor_end_date_in_past') == 'success':
            log = 'Supervisor end date in past'
        return log
    if previous_status == 'Success' and get_message() != '':
        message = previous_message.replace('Successfully', 'Partially')
        return message+', ' + get_message()

    return previous_message if get_message() == '' else previous_message+', ' + get_message()


def get_supervisor_properties(dag_run):
    previous_status = rail.result('get_message_from_log')[0]['status']
    status = 'Exception' if ((get_task_state('log_supervisor_not_present') == 'success'
                              or get_task_state('log_supervisor_end_date_in_past') == 'success') and previous_status != 'Error') else previous_status
    return {
        'employeeid': dag_run.conf['employeeid'],
        'firstname': dag_run.conf['firstname'],
        'lastname': dag_run.conf['lastname'],
        'status': status
    }


def get_default_timeoff_policy_set_schedule_for_timeofftype(dag_run):
    return {
        "timeOffTypeUri": dag_run.conf['timeofftypeuri']
    }


def put_user_timeoff_policy_schedule(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeofftypeuri']
        },
        "policySetScheduleEntries": json.loads(rail.result('get_all_policy_to_assign'))
    }


def put_user_timeoff_policy_schedule_rehire(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeofftypeuri']
        },
        "policySetScheduleEntries": json.loads(rail.result('get_default_policy_set_rehire'))
    }


def update_employee_date_range(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "dateRange": {
            "startDate": rail.result('get_user_info')['userDetails']['employmentDateRange']['startDate'],
            "endDate": null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def update_employee_date_range_delete(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "dateRange": {
            "startDate": rail.result('get_user_info')['userDetails']['employmentDateRange']['startDate'],
            "endDate": get_replicon_date(dag_run.conf['terminationdate']),
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def get_balance_summary_for_account(dag_run):
    end_date = get_replicon_date(dag_run.conf['terminationdate'])
    return {
        "account": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuriaccural']
        },
        "asOfDate": end_date
    }


def get_data_for_all_timeoff_after_the_enddate(dag_run):
    end_date = get_replicon_date(dag_run.conf['terminationdate'])
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:time-off-list-column:time-off"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "dateRange": {
                            "startDate": end_date
                        },
                        "dateTimeUtc": null
                    },
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-owner"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uri": dag_run.conf['useruri']
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


def put_user_timeoff_policy_accrual_policy(dag_run, config):
    timeoff_policy = []
    timeoff_policy_line = []
    final_time_off_policy = []
    policy = json.loads(dag_run.conf['policyset'])
    end_date = datetime.strptime(
        dag_run.conf['terminationdate'], '%m/%d/%Y') if dag_run.conf['terminationdate'] else datetime.now()
    starting_balance_setto_uri = rail.find_first_by_attr_and_get_attr(rail.result(
        'get_all_scripts_timeOff_balance_eventscript'), "displayText", "Starting Balance Set To", "uri")
    for policy_set in policy:
        effective_date = datetime.strptime(
            str(policy_set['effectiveDate']['year'])+"/"+str(policy_set['effectiveDate']['month'])+"/"+str(policy_set['effectiveDate']['day']), "%Y/%m/%d")
        if effective_date < end_date:
            timeoff_policy.append(
                {
                    "description": policy_set["description"],
                    "effectiveDate": {
                        "day": policy_set["effectiveDate"]["day"],
                        "month": policy_set["effectiveDate"]["month"],
                        "year": policy_set["effectiveDate"]["year"]
                    },
                    "policySet": policy_set["policySet"]
                })

    if (rail.result("get_balance_summary_for_account")["account"]["timeOffType"]["displayText"] in config.default_time_off_policy
        and dag_run.conf.get('is_employee_transerfred', "No") == "Yes"):
        timeoff_policy_line.append(
            {
                "timeOffBalanceEventScripts": [{
                    "script": {
                        "description": "Set initial balance for the first day of a policy",
                        "name": "Starting Balance Set To",
                        "uri": starting_balance_setto_uri
                    },
                    "additionalParameters": [{
                        "keyUri": "urn:replicon:script-key:parameter:amount",
                        "value": {
                            "number": 0
                        }
                    }]
                }]
            })

    elif rail.result("get_balance_summary_for_account")["account"]["timeOffType"]["displayText"] in config.accural_policy:
        timeoff_policy_line.append(
            {
                "timeOffBalanceEventScripts": [{
                    "script": {
                        "description": "Set initial balance for the first day of a policy",
                        "name": "Starting Balance Set To",
                        "uri": starting_balance_setto_uri
                    },
                    "additionalParameters": [{
                        "keyUri": "urn:replicon:script-key:parameter:amount",
                        "value": {
                            "number": rail.result("get_balance_summary_for_account")["timeRemaining"]
                        }
                    }]
                }]
            })
    else:
        timeoff_policy_line.append(
            {
                "timeOffBalanceEventScripts": [{
                    "script": {
                        "description": "Set initial balance for the first day of a policy",
                        "name": "Starting Balance Set To",
                        "uri": starting_balance_setto_uri
                    },
                    "additionalParameters": [{
                        "keyUri": "urn:replicon:script-key:parameter:amount",
                        "value": {
                            "number": 0
                        }
                    }]
                }]
            })

    timeoff_policy.append(
        {
            "description": "Added by Integration on " + datetime.strftime(end_date, "%m/%d/%Y"),
            "effectiveDate": {
                "day": end_date.day,
                "month": end_date.month,
                "year": end_date.year
            },
            "policySet": timeoff_policy_line[0]
        })

    for policy in timeoff_policy:
        final_time_off_policy.append(ast.literal_eval(
            str(policy).replace("'script'", "'scriptTarget'")))

    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuriaccural']
        },
        "policySetScheduleEntries": final_time_off_policy
    }


def create_timeOff_delete_batch():
    return {
        "timeOffUris": rail.result("get_data_forall_timeoff_after_the_enddate")
    }


def execute_timeOff_delete_batch():
    return {
        "timeOffDeleteBatchUri": rail.result("create_timeOff_delete_batch")
    }


def is_employee_type_changed(dag_run):
    current_employee_type = rail.result("get_user_info")[
        "employeeTypeGroupSchedule"][-1]["employeeTypeGroup"]["displayText"]
    file_employee_type = dag_run.conf["employee_type"]

    return current_employee_type != file_employee_type

def is_status_leave(dag_run):
    return dag_run.conf['positionstatus'] in ["Leave"]


def get_data_for_all_future_timeoff_after_the_enddate(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:time-off-list-column:time-off"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": dag_run.conf['todays_date']
                        }
                    },
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-owner"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uri": dag_run.conf['useruri']
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-type"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uri": rail.result('get_specfic_time_off_types')[0]['uri']
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def apply_sso_modifications(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri'],
        },
        "modifications": {
            "securitySettingsToApply": {
                "loginEnabled": "true",
                "forcePasswordChange": "false",
                "loginName": dag_run.conf['loginname'],
                "ssoName": dag_run.conf['loginname'],
                "password": null,
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "userSSONameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name"
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def is_hiredate_present(dag_run):
    return bool(dag_run.conf['hiredate'])


def is_user_disabled():
    return not rail.result("get_user_info")['userDetails']['isEnabled']


def is_end_date_present():
    return bool(rail.result("get_user_info")['userDetails']['employmentDateRange']['endDate'])

def get_cost_normalization_payload_add(dag_run):
    return {
        "userUri": rail.result("add_new_user")["uri"],
        "costNormalizationRuleUri": dag_run.conf['cnr_weekly_rule_uri'],
        "dateRange": {
            "startDate": get_replicon_date(dag_run.conf['hiredate']) or get_today_date(),
            "endDate": null,
        }
}
def get_previous_cost_normalization_start_date():
    current_cost_normalizations = rail.result('get_current_assigned_cost_normalization')['entries']
    if not current_cost_normalizations:
        return get_today_date()
    return current_cost_normalizations[-1]['effectiveDate']

def get_cost_normalization_payload_update(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "costNormalizationRuleUri": dag_run.conf['cnr_weekly_rule_uri'],
        "dateRange": {
            "startDate": get_today_date(),
            "endDate": null,
        } if test_exempt_employee_type(dag_run) else {
            "startDate": get_previous_cost_normalization_start_date(),
            "endDate": get_today_date(),
        }
}


def is_location_updated(loaction_mapper, location_1, location_2):
    return (location_2 in loaction_mapper and location_1 not in loaction_mapper)

def update_punch_entry_policy_location(dag_run, config):
    current_policy = rail.result("get_assigned_policy_to_user")[
                0]["policySet"]["displayText"]
    if not current_policy:
        current_policy = ""
    current_location_assigned = rail.result("get_effective_group_membership_for_user").get('locations',{})
    current_location_name = current_location_assigned[0].get("location",{}).get("location",{}).get("displayText", None)

    if current_location_name not in config.ca_locations and dag_run.conf['locationdescription'] in config.ca_locations:
        final_policy = dag_run.conf["california_punch_policy"] if "dummy" not in current_policy.lower() else dag_run.conf["dummy_ca_policy"]
    if current_location_name in config.ca_locations and dag_run.conf['locationdescription'] not in config.ca_locations:
        final_policy = dag_run.conf["avenu_punch_policy"] if "dummy" not in current_policy.lower() else dag_run.conf["dummy_punch_policy"]

    return {
        "userUri": rail.result('get_user_info')['userDetails']["uri"],
        "policySetUri": final_policy
    }
