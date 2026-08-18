from datetime import datetime
import hashlib
import ast
import json
import uuid
import rail
from airflow.models import Variable

null = None


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_conf():
    return rail.get_current_context()['dag_run'].conf


def get_user_uri():
    return get_conf()['useruri']


def get_today_date():
    now = datetime.now()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])


def get_process_each_record_conf(item, config):

    user_sync_mapper = ast.literal_eval(Variable.get(config.user_sync_mapper))

    def get_timesheet_template_name(code):
        if code == 'CA':
            data = list(filter(lambda x: x['type'] == 'timesheet_template' and x['location']== 'CA'
                    and x['code'] == item['employeetypecode'], user_sync_mapper))
            return data[0]['name'] if data else None
        data = list(filter(lambda x: x['type'] == 'timesheet_template' and x['location']== 'Non-CA'
                and x['code'] == item['employeetypecode'], user_sync_mapper))
        return data[0]['name'] if data else None

    def get_payrule_script_name(code):
        data = list(filter(lambda x: x['type'] == 'payrule', user_sync_mapper))
        result = rail.find_first_by_attr_and_get_attr(
            data, 'statecode', code, 'payrule')
        if not result:
            return rail.find_first_by_attr_and_get_attr(data, 'statecode', 'Rest', 'payrule')
        return result

    def get_work_week_uri():
        workweekstartday = ((item['workweek']).split(' ')[0]).lower()
        return "urn:replicon:day-of-week:" + workweekstartday

    locationuri = rail.find_first_by_attr_and_get_attr(rail.result('get_all_locations'), 'displayText', item['homezip'], 'uri')\
        if item['worklocation'] == 'Remote' else rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_locations'), 'displayText', item['workzip'], 'uri')

    timesheettemplate = get_timesheet_template_name('CA') if (item['worklocation'] == 'Remote' and item['homestate'] == 'CA')\
        or (item['worklocation'] == 'Client' and item['workstate'] == 'CA') else get_timesheet_template_name('Non-CA')

    timesheettemplateuri = rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'),
        'displayText', get_timesheet_template_name('CA'), 'uri')\
        if (item['worklocation'] == 'Remote' and item['homestate'] == 'CA') or (item['worklocation'] == 'Client' and item['workstate'] == 'CA') else\
        rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'), 'displayText', get_timesheet_template_name('Non-CA'), 'uri')

    payrulescripturi = rail.find_first_by_attr_and_get_attr(rail.result('get_all_payrule_scripts'),
        'displayText', get_payrule_script_name(item['homestate']), 'uri')\
        if item['worklocation'] == 'Remote' else rail.find_first_by_attr_and_get_attr(rail.result(
        'get_all_payrule_scripts'), 'displayText', get_payrule_script_name(item['workstate']), 'uri')


    return {
        **dict(item.items()),
        **{
        'loginname': item['personid'],
        'employeeid': item['personid'],
        'homeaddressuri': rail.result("get_user_oefs")['homeaddressuri'],
        'homecityuri': rail.result("get_user_oefs")['homecityuri'],
        'workstreeturi': rail.result("get_user_oefs")['workstreeturi'],
        'workcityuri': rail.result("get_user_oefs")['workcityuri'],
        'workstateuri': rail.result("get_user_oefs")['workstateuri'],
        'workzipuri': rail.result("get_user_oefs")['workzipuri'],
        'homezipuri': rail.result("get_user_oefs")['homezipuri'],
        'emergencycontactrelationshipuri': rail.result("get_user_oefs")['emergencycontactrelationshipuri'],
        'emergencycontactnumberuri': rail.result("get_user_oefs")['emergencycontactnumberuri'],
        'emergencycontactfirstnameuri': rail.result("get_user_oefs")['emergencycontactfirstnameuri'],
        'emergencycontactlastnameuri': rail.result("get_user_oefs")['emergencycontactlastnameuri'],
        'worklocationuri': rail.result("get_user_oefs")['worklocationuri'],
        'tagworklocationuri': rail.result('get_user_oef_dropdown_value')['remote_uri'] if item['worklocation'] == 'Remote'
        else rail.result('get_user_oef_dropdown_value')['client_uri'],
        'homestateuri': rail.result("get_user_oefs")['homestateuri'],
        'cellphoneuri': rail.result("get_user_oefs")['cellphoneuri'],
        'homephoneuri': rail.result("get_user_oefs")['homephoneuri'],
        'benefitanniversarydateuri': rail.result("get_user_oefs")['benefitanniversarydateuri'],
        'middlenameuri': rail.result("get_user_oefs")['middlenameuri'],
        'adpiduri': rail.result("get_user_oefs")['adpiduri'],
        'burdenuri': rail.result("get_user_oefs")['burdenuri'],
        'enduseruri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'), 'displayText', 'End User', 'uri'),
        'timesheetapproveruri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'), 'displayText', 'Timesheet Approver', 'uri'),
        'timezoneuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_timezones'), 'displayText', item['timezone'], 'uri'),
        'departmenturi': rail.find_first_by_attr_and_get_attr(rail.result('get_all_departments'), 'displayText', item['departmentname'], 'uri')
            if item['departmentname'] else null,
        'employeetypeuri':  rail.find_first_by_attr_and_get_attr(rail.result('get_all_employeetypes'), 'code', item['employeetypecode'], 'uri'),
        'locationuri': locationuri,
        'timesheettemplate': timesheettemplate,
        'timesheettemplateuri':timesheettemplateuri,
        'workweekuri': get_work_week_uri(),
        'payrulescript': get_payrule_script_name(item['homestate']) if item['worklocation'] == 'Remote' else get_payrule_script_name(item['workstate']),
        'payrulescripturi': payrulescripturi,
        'timeofftemplate': config.timeofftemplate,
        'timesheet_period_schedule': config.timesheet_period_schedule,
        'schedule_policy': config.schedule_policy,
        'holidaycalenderuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calenders'), 'displayText', item['holidaycalender'], 'uri'),
        }
    }


def get_user_data_payload(dag_run):
    return{
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
        date = datetime.strptime(date_str, '%m-%d-%Y')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def get_put_user_payload(dag_run):
    # pylint: disable=too-many-branches

    oefs = []

    def add_dropdown_oef(name):
        oefs.append(
            {
                "definition": {
                    "uri": dag_run.conf[f'{name}uri'],
                    "name": null
                },
                "tag": {
                    "uri": dag_run.conf['tagworklocationuri'],
                    "slug": null,
                    "tagName": null
                },
                "numericValue": null,
                "textValue": null,
                "fileValue": null,
                "jsonValue": null
            }
        )

    def add_text_oef(name):
        oefs.append(
            {
                "definition": {
                    "uri": dag_run.conf[f'{name}uri'],
                    "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": dag_run.conf[f'{name}'],
                "fileValue": null,
                "jsonValue": null
            }
        )

    if dag_run.conf['homeaddress']:
        add_text_oef('homeaddress')
    if dag_run.conf['homecity']:
        add_text_oef('homecity')
    if dag_run.conf['homezip']:
        add_text_oef('homezip')
    if dag_run.conf['workstreet']:
        add_text_oef('workstreet')
    if dag_run.conf['workcity']:
        add_text_oef('workcity')
    if dag_run.conf['workstate']:
        add_text_oef('workstate')
    if dag_run.conf['workzip']:
        add_text_oef('workzip')
    if dag_run.conf['emergencycontactrelationship']:
        add_text_oef('emergencycontactrelationship')
    if dag_run.conf['emergencycontactfirstname']:
        add_text_oef('emergencycontactfirstname')
    if dag_run.conf['emergencycontactlastname']:
        add_text_oef('emergencycontactlastname')
    if dag_run.conf['emergencycontactnumber']:
        add_text_oef('emergencycontactnumber')
    if dag_run.conf['homestate']:
        add_text_oef('homestate')
    if dag_run.conf['cellphone']:
        add_text_oef('cellphone')
    if dag_run.conf['homephone']:
        add_text_oef('homephone')
    if dag_run.conf['worklocation']:
        add_dropdown_oef('worklocation')
    if dag_run.conf['burden']:
        add_text_oef('burden')
    if dag_run.conf['adpid']:
        add_text_oef('adpid')
    if dag_run.conf['middlename']:
        add_text_oef('middlename')
    if dag_run.conf['benefitanniversarydate']:
        add_text_oef('benefitanniversarydate')

    def update_enddate():
        enddate = datetime.strptime(str(dag_run.conf['lastassignmentenddate']), "%m-%d-%Y")
        startdate = datetime.strptime(str(dag_run.conf['originalstartdate']), "%m-%d-%Y")
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
            "emailAddress": dag_run.conf['timesheetemail'],
            "employeeId": dag_run.conf['employeeid'],
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
                    "effectiveDate": null
                }
            ],
            "workWeekStartDayUri": dag_run.conf['workweekuri'],
            "employmentDateRange": {
                "startDate": get_replicon_date(dag_run.conf['originalstartdate']),
                "endDate": get_replicon_date(dag_run.conf['lastassignmentenddate']) if dag_run.conf['lastassignmentenddate'] and update_enddate() else null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:replicon"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['loginname'],
                "SSOName": null,
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
            "costRateSchedule": null if not dag_run.conf['hourlypayrate'] else {
                "initialHourlyRate": {
                    "amount": dag_run.conf['hourlypayrate'],
                    "currency": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":currency:1",
                        "name": null,
                        "symbol": null
                    }
                },
                "scheduleEntries": []
            },
            "payrollRateSchedule": null,
            "defaultBillingRate": null if not dag_run.conf['netbillrate'] else{
                "amount": dag_run.conf['netbillrate'],
                "currency": {
                    "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":currency:1",
                    "name": null,
                    "symbol": null
                }
            },
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
                    "effectiveDate": null
                }
            ],
            "divisionSchedule": [],
            "costCenterSchedule": [],
            "serviceCenterSchedule": [],
            "departmentGroupSchedule": [] if not dag_run.conf['departmentname'] else [
                {
                    "departmentGroup": {
                        "uri": dag_run.conf['departmenturi'],
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
                        "uri": dag_run.conf['employeetypeuri'],
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
                        "name": dag_run.conf['timesheet_period_schedule']
                    },
                    "effectiveDate": null
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
                    "effectiveDate": null
                }
            ],
            "displayNameParameter": null,
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": oefs
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


def get_remove_timeoff_payload():
    return {
        "userUri": rail.result('add_new_user')['uri'],
        "timeOffTypeUris": []
    }


def get_process_time_off_assignment_conf(dag_run, status):
    keys_to_fiter = ('loginname', 'employeeid', 'firstname', 'lastname', 'employeetype', 'employeetypecode', 'worklocation',
    'homestate', 'homecity', 'homezip', 'workstate', 'workcity', 'workzip', 'todays_date')
    return{
        **{ k: v for k, v in dag_run.conf.items() if k in keys_to_fiter  },
        **{
        'usertype': status,
        'useruri': rail.result('add_new_user')['uri'] if status == 'new_user' else dag_run.conf['useruri'],
        'log_exception': rail.result('add_user_exception_log') if status == 'new_user' else rail.result('update_user_exception_log'),
        'log_error': rail.result('add_user_error_logs') if status == 'new_user' else rail.result('update_user_error_logs'),
        }
    }


def get_default_timeoff_policy_schedule_payload(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeofftypeuri']
        }
    }


def get_user_timeoff_policy_payload(dag_run):
    return{
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeofftypeuri']
        },
        "policySetScheduleEntries": json.loads(rail.result('policy_to_assign'))
    }


def create_departments_in_replicon(item):
    return {
        "hierarchy": [
            {
                "target": {
                    "uri": null,
                    "parent": {
                        "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_department_grps'), 'displayText', 'Malten Silver Inc.', 'uri'),
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "parameterCorrelationId": null,
                "modificationToApply": {
                    "name": item['departmentname'],
                    "codeToApply": {
                        "value": item['departmentcode']
                    },
                    "descriptionToApply": null,
                    "isEnabled": True
                }
            }
        ],
        "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def create_employeetypes_in_replicon(item):
    return {
        "hierarchy": [
            {
                "target": null,
                "parameterCorrelationId": null,
                "modificationToApply": {
                    "name": item['employeetype'],
                    "codeToApply": {
                        "value": item['employeetypecode']
                    },
                    "descriptionToApply": null,
                    "isEnabled": True
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
                    "name": item['zipcode'],
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": True
                }
            }
        ],
        "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
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
                    "text": dag_run.conf['supervisorcode'],
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


def put_timeoff_assignment_for_user(dag_run):
    return{
        "userUri": dag_run.conf['useruri'],
        "timeOffTypeUris": rail.result('get_time_off_type_uris')
    }


def get_process_time_off_policy_new_user_conf(item, dag_run):
    keys_to_fiter = ('loginname', 'employeeid', 'firstname', 'lastname','useruri', 'employeetype', 'employeetypecode', 'worklocation',
    'homestate', 'homecity', 'homezip', 'workstate', 'workcity', 'workzip','usertype', 'log_exception', 'log_error', 'todays_date')
    return {
        **{ k: v for k, v in dag_run.conf.items() if k in keys_to_fiter  },
        **{
            'timeofftypename': item['timeofftypename'],
            'timeofftypeuri': item['timeofftypeuri']
        }
    }


def apply_user_modifications(dag_run):
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-branches
    def update_user_details():

        def update_enddate():
            if dag_run.conf['lastassignmentenddate']:
                enddate = datetime.strptime(str(dag_run.conf['lastassignmentenddate']), "%m-%d-%Y")
                startdate = datetime.strptime(str(dag_run.conf['originalstartdate']), "%m-%d-%Y")
                return bool(enddate > startdate)
            return True

        is_first_name_changed = dag_run.conf['firstname'] and (dag_run.conf['firstname'] != rail.result(
            'get_user_info')['userDetails']['firstName'])
        is_last_name_changed = dag_run.conf['lastname'] and (dag_run.conf['lastname'] != rail.result(
            'get_user_info')['userDetails']['lastName'])
        is_email_changed = dag_run.conf['timesheetemail'] and (dag_run.conf['timesheetemail'] != rail.result(
            'get_user_info')['userDetails']['emailAddress'])
        assigned_enddate = rail.result('get_user_info')[
            'userDetails']['employmentDateRange']['endDate']
        is_enddate_changed = (True if not assigned_enddate else
            (bool(dag_run.conf['lastassignmentenddate'] != get_date_from_replicon_date(assigned_enddate).strftime("%m-%d-%Y"))))\
            if update_enddate() else False

        if get_task_state('enable_login') != 'success':
            assigned_startdate = rail.result('get_user_info')[
                'userDetails']['employmentDateRange']['startDate']
            is_startdate_changed = dag_run.conf['originalstartdate'] != get_date_from_replicon_date(
                assigned_startdate).strftime("%m-%d-%Y")
        else:
            is_startdate_changed = False

        def get_first_name():
            if is_first_name_changed:
                return dag_run.conf['firstname']
            return null

        def get_last_name():
            if is_last_name_changed:
                return dag_run.conf['lastname']
            return null

        def get_email():
            if is_email_changed:
                return{
                    "emailAddress": dag_run.conf['timesheetemail']
                }
            return null

        def get_startdate():
            if is_startdate_changed:
                return {
                    "date": get_replicon_date(dag_run.conf['originalstartdate'])
                }
            return null

        def get_enddate():
            if is_enddate_changed:
                return {
                    "date": get_replicon_date(dag_run.conf['lastassignmentenddate'])
                }
            return null

        return {
            "firstName": get_first_name(),
            "lastName": get_last_name(),
            "emailAddress": get_email(),
            "employmentStartDate": get_startdate(),
            "employmentEndDate": get_enddate(),
        } if is_first_name_changed or is_last_name_changed or is_email_changed or is_enddate_changed or is_startdate_changed else null

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

    def update_workweek():
        assigned_workweek = rail.result('get_user_info')['userDetails']['workWeekStartDay']
        if not assigned_workweek:
            if dag_run.conf['workweek']:
                return {
                "workWeekStartDayUri": dag_run.conf['workweekuri']
            }
            return null
        if dag_run.conf['workweek'] and (dag_run.conf['workweekuri'] != rail.result('get_user_info')['userDetails']['workWeekStartDay']['uri']):
            return {
                "workWeekStartDayUri": dag_run.conf['workweekuri']
            }
        return null

    def update_holiday_calender():
        assigned_holiday_calender = rail.result('get_user_info')['holidayCalendar']
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
        if rail.find_first_by_attr_and_get_attr(rail.result('get_user_info')['permissionSets'], 'displayText', 'End User', 'displayText') != 'End User':
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
            if dag_run.conf['departmentname'] and dag_run.conf['departmentcode']:
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

        if (dag_run.conf['departmentname'] and dag_run.conf['departmentcode']) and \
                dag_run.conf['departmentname'] != assigned_department_grp[-1]['departmentGroup']['displayText']:
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

    def update_employee_type_grp():
        assigned_employee_type_grp = rail.result(
            "get_user_info")['employeeTypeGroupSchedule']
        if not assigned_employee_type_grp:
            if dag_run.conf['employeetype'] and dag_run.conf['employeetypecode']:
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

        if (dag_run.conf['employeetype'] and dag_run.conf['employeetypecode']) and \
                dag_run.conf['employeetype'] != assigned_employee_type_grp[-1]['employeeTypeGroup']['displayText']:
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
        # pylint: disable=too-many-return-statements
        assigned_locations = rail.result("get_user_info")['locationSchedule']
        if dag_run.conf['worklocation'] == 'Remote':
            if not assigned_locations:
                if dag_run.conf['homezip']:
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
            if dag_run.conf['homezip'] and dag_run.conf['homezip'] != assigned_locations[-1]['location']['displayText']:
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
        else:
            if not assigned_locations:
                if dag_run.conf['workzip']:
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
            if dag_run.conf['homezip'] and dag_run.conf['workzip'] != assigned_locations[-1]['location']['displayText']:
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

    oefs = []

    def add_dropdown_oef(name):
        oefs.append(
            {
                "definition": {
                    "uri": dag_run.conf[f'{name}uri'],
                    "name": null
                },
                "tag": {
                    "uri": dag_run.conf['tagworklocationuri'],
                    "slug": null,
                    "tagName": null
                },
                "numericValue": null,
                "textValue": null,
                "fileValue": null,
                "jsonValue": null
            }
        )

    def add_text_oef(name):
        oefs.append(
            {
                "definition": {
                    "uri": dag_run.conf[f'{name}uri'],
                    "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": dag_run.conf[f'{name}'],
                "fileValue": null,
                "jsonValue": null
            }
        )

    if dag_run.conf['homeaddress'] and dag_run.conf['homeaddress'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Home Address', 'textValue'):
        add_text_oef('homeaddress')

    if dag_run.conf['homecity'] and dag_run.conf['homecity'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Home City', 'textValue'):
        add_text_oef('homecity')

    if dag_run.conf['homezip'] and dag_run.conf['homezip'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Home Zip', 'textValue'):
        add_text_oef('homezip')

    if dag_run.conf['workstreet'] and dag_run.conf['workstreet'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Work Street', 'textValue'):
        add_text_oef('workstreet')

    if dag_run.conf['workcity'] and dag_run.conf['workcity'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Work City', 'textValue'):
        add_text_oef('workcity')

    if dag_run.conf['workstate'] and dag_run.conf['workstate'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Work State', 'textValue'):
        add_text_oef('workstate')

    if dag_run.conf['workzip'] and dag_run.conf['workzip'] !=\
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Work Zip', 'textValue'):
        add_text_oef('workzip')

    if dag_run.conf['emergencycontactrelationship'] and dag_run.conf['emergencycontactrelationship'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText',
            'Emergency Contact Relationship', 'textValue'):
        add_text_oef('emergencycontactrelationship')

    if dag_run.conf['emergencycontactnumber'] and dag_run.conf['emergencycontactnumber'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Emergency Contact Number', 'textValue'):
        add_text_oef('emergencycontactnumber')

    if dag_run.conf['emergencycontactlastname'] and dag_run.conf['emergencycontactlastname'] !=\
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Emergency Contact Last Name', 'textValue'):
        add_text_oef('emergencycontactlastname')

    if dag_run.conf['emergencycontactfirstname'] and dag_run.conf['emergencycontactfirstname'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Emergency Contact First Name', 'textValue'):
        add_text_oef('emergencycontactfirstname')

    if dag_run.conf['homestate'] and dag_run.conf['homestate'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Home State', 'textValue'):
        add_text_oef('homestate')

    if dag_run.conf['cellphone'] and dag_run.conf['cellphone'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Cell Number', 'textValue'):
        add_text_oef('cellphone')

    if dag_run.conf['homephone'] and dag_run.conf['homephone'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Telephone Number', 'textValue'):
        add_text_oef('homephone')

    if dag_run.conf['worklocation'] and dag_run.conf['tagworklocationuri'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Work Location', 'tag.uri'):
        add_dropdown_oef('worklocation')

    if dag_run.conf['burden'] and dag_run.conf['burden'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Burden', 'textValue'):
        add_text_oef('burden')

    if dag_run.conf['middlename'] and dag_run.conf['middlename'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Middle Name', 'textValue'):
        add_text_oef('middlename')

    if dag_run.conf['benefitanniversarydate'] and dag_run.conf['benefitanniversarydate'] != \
            rail.find_first_by_attr_and_get_attr(rail.result('get_current_oef_values'), 'definition.displayText', 'Benefit Anniversary Date', 'textValue'):
        add_text_oef('benefitanniversarydate')

    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "timezoneToApply": update_timezone(),
            "workWeekStartToApply": update_workweek(),
            "holidayCalendarToApply": update_holiday_calender(),
            "locationScheduleToApply": update_location_grp(),
            "departmentGroupScheduleToApply": update_department_grp(),
            "employeeTypeGroupScheduleToApply": update_employee_type_grp(),
            "permissionSetsToApply": update_permission_set(),
            'policySetsToApply': update_policy_set(),
            "payRulesScheduleModifications": update_payrule_script(),
            "userDetailsToApply": update_user_details(),
            "objectExtensionFieldsToApply": oefs
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def update_billing_rate(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "hourlyRate": {
            "amount": dag_run.conf['netbillrate'],
            "currency": {
                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":currency:1",
                "name": null,
                "symbol": null
            }
        },
        "effectiveDate": null
    }


def test_billing_rate(dag_run):
    assigned_billingrate = rail.result('get_user_info')['defaultBillingRate']
    if not assigned_billingrate:
        return True
    if float(dag_run.conf['netbillrate']) != float(assigned_billingrate['effectiveBillingRate']['value']['amount']):
        return True
    return False


def test_hourly_rate(dag_run):
    assigned_hourly_rate = rail.result('get_user_info')['costRateSchedule']
    if not assigned_hourly_rate:
        return True
    if float(dag_run.conf['hourlypayrate']) != float(assigned_hourly_rate[-1]['hourlyRate']['amount']):
        return True
    return False


def update_hourly_rate(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "hourlyRate": {
            "amount": dag_run.conf['hourlypayrate'],
            "currencyUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":currency:1"
        },
        "dateRange": {
            "startDate": dag_run.conf['todays_date'],
            "endDate": null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def test_employeetypechange(dag_run, config):
    user_sync_mapper = ast.literal_eval(Variable.get(config.user_sync_mapper))
    sicktimeoff_details = list(filter(
        lambda x: x['type'] == 'time_off' and x['timeoffname'] == 'Sick Time Off', user_sync_mapper))
    sicktimeoff_emp_grp = list(details['employeetype'] for details in sicktimeoff_details)
    paid_time_off_details = list(filter(
        lambda x: x['type'] == 'time_off' and x['timeoffname'] == 'Paid Time Off', user_sync_mapper))
    paid_time_off_grp = list(details['employeetype'] for details in paid_time_off_details)
    assigned_employee_type_grp = rail.result(
        "get_user_info")['employeeTypeGroupSchedule']
    if not assigned_employee_type_grp:
        return True
    if dag_run.conf['employeetype'] in sicktimeoff_emp_grp:
        if assigned_employee_type_grp[-1]['employeeTypeGroup']['displayText'] not in sicktimeoff_emp_grp:
            return True
        return False
    if dag_run.conf['employeetype'] in paid_time_off_grp:
        if assigned_employee_type_grp[-1]['employeeTypeGroup']['displayText'] not in paid_time_off_grp:
            return True
        return False
    return False


def test_zipcode_change(dag_run, config):
    user_sync_mapper = ast.literal_eval(Variable.get(config.user_sync_mapper))
    sicktimeoff_details = list(filter(
        lambda x: x['type'] == 'time_off' and x['timeoffname'] == 'Sick Time Off', user_sync_mapper))
    sicktimeoff_emp_grp = list(details['employeetype']
                               for details in sicktimeoff_details)

    if dag_run.conf['employeetype'] in sicktimeoff_emp_grp:
        assigned_locations = rail.result("get_user_info")['locationSchedule']
        if not assigned_locations and (bool(dag_run.conf['homezip']) if dag_run.conf['worklocation'] == 'Remote' else bool(dag_run.conf['workzip'])):
            return True
        if dag_run.conf['worklocation'] == 'Remote':
            if dag_run.conf['homezip'] and dag_run.conf['homezip'] != assigned_locations[-1]['location']['displayText']:
                return True
            return False
        if dag_run.conf['workzip'] and dag_run.conf['workzip'] != assigned_locations[-1]['location']['displayText']:
            return True
        return False
    return False


def get_create_md5_data(item):
    if not item:
        return []
    res = {
        **dict(item.items()),
        **{
        'md5': hashlib.md5((item["personid"]+","+item["firstname"]+","+item["lastname"]+","
                            + item["middlename"]+"," + item["supervisorname"] +
                            "," + item["supervisorcode"]
                            + item["homestate"]+"," + item["homezip"] +
                            "," + item["homeaddress"]
                            + item["homecity"]+"," +
                            item["timesheetemail"]+"," + item["homephone"]
                            + item["cellphone"]+"," + item["emergencycontactnumber"] +
                            "," + item["emergencycontactfirstname"]
                            + item["emergencycontactlastname"]+"," +
                            item["emergencycontactrelationship"] +
                            "," + item["originalstartdate"]
                            + item["lastassignmentenddate"]+"," +
                            item["employeetype"]+"," +
                            item["employeetypecode"]
                            + item["departmentname"]+"," +
                            item["departmentcode"] +
                            "," + item["worklocation"]
                            + item["workstreet"]+"," +
                            item["workcity"]+"," + item["workstate"]
                            + item["workzip"]+"," + item["timezone"] +
                            "," + item["benefitanniversarydate"]
                            + item["adpid"]+"," + item["netbillrate"] +
                            "," + item["hourlypayrate"]
                            + item["burden"]+"," + item["workweek"] +
                            "," + item["holidaycalender"]
                            ).encode('utf-8')).hexdigest()
        }
    }

    return {k: v if v is not None else '' for k, v in res.items()}


def get_user_time_off_policy_summary(dag_run):
    return {
        "userUri": dag_run.conf['useruri']
    }


def test_timeoff_availablity():
    data = rail.result('get_timeoff_type_list')
    check1 = rail.find_first_by_attr_and_get_attr(
        data, 'timeofftypename', 'Not Available', 'timeofftypename')
    check2 = rail.find_first_by_attr_and_get_attr(
        data, 'timeofftypeuri', 'Not Available', 'timeofftypeuri')
    return bool(check1 != 'Not Available' and check2 != 'Not Available')


def log_timeoff_not_available(dag_run):
    data = rail.result('get_timeoff_type_list')
    check1 = rail.find_first_by_attr_and_get_attr(
        data, 'timeofftypename', 'Not Available', 'timeofftypename')
    if check1 == 'Not Available':
        return "Time off Policy for " + dag_run.conf['homezip'] + ' zipcode not available in mapper' if dag_run.conf['worklocation'] == 'Remote' \
            else "Time off Policy for " + dag_run.conf['workzip'] + ' zipcode not available in mapper'
    check2 = rail.find_first_by_attr_and_get_attr(
        data, 'timeofftypeuri', 'Not Available', 'timeofftypeuri')
    if check2 == 'Not Available':
        return "Time off type is not available in replicon"
    return "Time off type is not available in replicon"


def get_process_time_off_policy_update_rehire_user(item, dag_run):
    def get_policy(item):
        data = rail.result('get_user_time_off_policy_summary')[
            'policiesByTimeOffType']
        return rail.find_first_by_attr_and_get_attr(data, 'timeOffType.uri', item['timeofftypeuri'], 'policySetSchedule')

    keys_to_fiter = ('loginname', 'employeeid', 'firstname', 'lastname','useruri', 'employeetype', 'employeetypecode', 'worklocation',
    'homestate', 'homecity', 'homezip', 'workstate', 'workcity', 'workzip','usertype', 'log_exception', 'log_error', 'todays_date')
    return {
         **{ k: v for k, v in dag_run.conf.items() if k in keys_to_fiter  },
         **{
            'timeofftypename': item['timeofftypename'],
            'timeofftypeuri': item['timeofftypeuri'],
            'policy': get_policy(item)
            }
    }


def get_default_timeoff_policy_set_schedule_for_timeofftype(dag_run):
    return {
        "timeOffTypeUri": dag_run.conf['timeofftypeuri']
    }


def put_user_timeoff_policy_schedule(dag_run):
    return{
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeofftypeuri']
        },
        "policySetScheduleEntries": json.loads(rail.result('get_all_policy_to_assign'))
    }


def test_enddate(dag_run):
    assigned_enddate = rail.result('get_user_info')[
        'userDetails']['employmentDateRange']['endDate']
    if not assigned_enddate:
        return False
    if datetime.strptime(get_date_from_replicon_date(assigned_enddate).strftime("%m-%d-%Y"),
        "%m-%d-%Y") < datetime.strptime(get_date_from_replicon_date(dag_run.conf['todays_date']).strftime("%m-%d-%Y"), "%m-%d-%Y"):
        return True
    return False


def check_enddate(dag_run):
    if dag_run.conf['lastassignmentenddate']:
        enddate = datetime.strptime(str(dag_run.conf['lastassignmentenddate']), "%m-%d-%Y")
        startdate = datetime.strptime(str(dag_run.conf['originalstartdate']), "%m-%d-%Y")
        return 'Enddate is prior to Startdate' if  enddate < startdate else ''
    return ''

def get_add_completion_message(dag_run):
    return ('User Added Partially, ' +str(rail.result('get_all_error_logs'))[1:-1])\
        if rail.result('get_all_error_logs') else (('User Added Partially, ' + str(rail.result('get_all_exception_logs'))[1:-1] + check_enddate(dag_run))\
        if rail.result('get_all_exception_logs') or check_enddate(dag_run) != '' else 'User Added Successfully')

def get_update_completion_message(dag_run):
    return ('User Updated Partially, ' + str(rail.result('get_all_error_logs'))[1:-1] + str(rail.result('get_all_success_logs'))[1:-1])\
        if rail.result('get_all_error_logs') else (('User Updated Partially, ' + str(rail.result('get_all_exception_logs'))[1:-1]+
        str(rail.result('get_all_success_logs'))[1:-1] + check_enddate(dag_run))\
        if rail.result('get_all_exception_logs') or check_enddate(dag_run) != '' \
        else ('User Updated Successfully' + str(rail.result('get_all_success_logs'))[1:-1]))

def get_update_severity(dag_run):
    supervisor_status = get_task_state('log_supervisor_not_present') == 'success'
    return 'Pending_User' if supervisor_status else ('Error' if rail.result('get_all_error_logs') else (
        'Exception' if rail.result('get_all_exception_logs') or check_enddate(dag_run) != '' else 'Update_Success'))

def get_add_severity(dag_run):
    supervisor_status = get_task_state('log_supervisor_not_present') == 'success'
    return 'Pending_User' if supervisor_status else ('Error' if rail.result('get_all_error_logs') else (
        'Exception' if rail.result('get_all_exception_logs') or check_enddate(dag_run) != '' else 'Add_Success'))

def test_valid_fields(dag_run):
    flag = True
    startdate = get_replicon_date(dag_run.conf['originalstartdate'])
    if not startdate:
        flag=False
    if dag_run.conf['lastassignmentenddate']:
        enddate = get_replicon_date(dag_run.conf['lastassignmentenddate'])
        if not enddate:
            flag=False
    if dag_run.conf['worklocation'] not in ['Remote','Client']:
        flag=False
    return flag

def get_invalid_fields_message(dag_run):
    log=[]
    startdate = get_replicon_date(dag_run.conf['originalstartdate'])
    if not startdate:
        log.append('Invalid format for Start Date')
    if dag_run.conf['lastassignmentenddate']:
        enddate = get_replicon_date(dag_run.conf['lastassignmentenddate'])
        if not enddate:
            log.append('Invalid format for End Date')
    if dag_run.conf['worklocation'] not in ['Remote','Client']:
        log.append('Worklocation is not present in Replicon')
    return str(log)[1:-1]

def get_supervisor_conf(item):
    return{
        **dict(item['properties'].items()),
        'enduseruri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'), 'displayText', 'End User', 'uri'),
        'timesheetapproveruri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'), 'displayText', 'Timesheet Approver', 'uri'),
    }

def get_supervisor_message_master_log():
    previous_message = rail.result('get_message_from_log')[0]['message']
    previous_status = rail.result('get_message_from_log')[0]['status']

    def get_message():
        log=''
        if get_task_state('log_supervisor_not_present') == 'success':
            log='Supervisor not present in replicon'
        if get_task_state('log_supervisor_end_date_in_past') == 'success':
            log='Supervisor end date in past'
        return log
    if previous_status == 'Success'and get_message() != '':
        message = previous_message.replace('Successfully', 'Partially')
        return message+', '+ get_message()

    return previous_message if get_message() == '' else previous_message+', '+ get_message()

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
