from datetime import datetime, date
from functools import lru_cache
import uuid
import pendulum
import rail
from deltek_northstar.user_sync_polaris_india.utils.python_callable import get_current_date, parse_date_json, get_updated_start_date

null = None
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
INSTANCE_DATE_FORMAT = "%Y-%m-%d"
EMAIL_DATE_FORMAT = "%m/%d/%Y"
ALL_NOTIFICATIONS = ['expense-sheet', 'pay-rule-script', 'project', 'time-off', 'user', 'time-punch-action', 'timesheet', 'time-entry-revision-group', 'holiday']
ENABLE_NOTIFICATIONS = {'time-off', 'user', 'time-punch-action', 'timesheet'}

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_dict_to_date(date_dict):
    if not date_dict:
        return None
    try:
        dt_str = f"{date_dict['year']}-{date_dict['month']}-{date_dict['day']}"
        return datetime.strptime(dt_str, INSTANCE_DATE_FORMAT)
    except:  # pylint: disable=bare-except
        return None

def get_today_date(config):
    now = pendulum.now(config.time_zone)
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

def get_supervisor_data_payload(dag_run):
    return {
        "users": [
            {
            "uri": null,
            "loginName": null,
            "employeeId": dag_run.conf['mgr_empl_id'],
            "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_supervisor_data_with_manager_id(dag_run):
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

@lru_cache(maxsize=8)
def get_dagrun_conf():
    return rail.get_dag_run_conf()

def get_conf_employee_id():
    if get_dagrun_conf():
        return get_dagrun_conf().get('employee_id')
    return null

def get_relations(get_last_run_date):
    empl_id = get_conf_employee_id()
    relations = [
        {
            "name": "TAXBLE_ENTITY_ID",
            "relation": "=",
            "value": "700"
        },
        {
            "name": "POLARIS_USER_FL",
            "relation": "=",
            "value": "Y"
        }
    ]
    if empl_id:
        relations.append({
            "name": "EMPL_ID",
            "relation": "=",
            "value": empl_id
        })
    if get_last_run_date:
        relations.append({
                    "name": "EMPL_TIME_STAMP",
                    "relation": "gt",
                    "value": get_last_run_date
                })
    return relations


def get_costpoint_payload(get_last_run_date):
    return {
        "filter": {
            "id": "it_polaris_empl",
            "where": [
            {
                "rsWhere": {
                "rsId": "XT_EMPL_SRC_POL_EMPL",
                "conditions": [
                    {
                    "joinWithParent": "N",
                    "relations": get_relations(get_last_run_date)
                    }
                ],
                "children": []
                }
            }
            ]
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


def get_user_data_by_loginname_payload(dag_run):
    return {
        "users": [
            {
            "uri": null,
            "loginName": dag_run.conf['email_id'],
            "employeeId": null,
            "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def get_user_data_payload(dag_run):
    return {
        "users": [
            {
            "uri": null,
            "loginName": null,
            "employeeId": dag_run.conf['empl_id'],
            "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_pay_group_name(rate_type):
    return "Salaried Fixed Hours" if rate_type == "S" else "Hourly"

def taxable_entity_name(item):
    return f"{item['taxble_entity_id']} - {item['taxble_entity_name']}"

def get_employee_type_name(employee_type):
    emp_type = ""
    if employee_type == "P":
        emp_type = "Part Time"
    if employee_type == "R":
        emp_type = "Regular"
    if employee_type == "T":
        emp_type = "Temporary"
    return emp_type

def get_licences_to_be_assigned(config, pay_period_code):
    licenses = rail.find_first_by_attr_and_get_attr(config.PAY_PERIOD_MAPPER, "Pay Period Code", pay_period_code, "Licenses", [])
    resp = []
    for license in licenses:
        if license == "TOE":
            resp.append("urn:replicon-saas:product:time-off-enterprise")
        if license == "WFM":
            resp.append("urn:replicon-saas:product:wfm-enterprise")
        if license == "Polaris PSA":
            resp.append("urn:replicon-saas:product:psm-enterprise-2")
    return resp

def get_mapper_dict(config, pay_period_code):
    mapper_dict = rail.find_first_by_attr_and_get_attr(config.PAY_PERIOD_MAPPER, "Pay Period Code", pay_period_code)
    return mapper_dict if mapper_dict else None

def get_mapper_values(config, pay_period_code, key):
    resp = get_mapper_dict(config, pay_period_code)
    return resp[key] if resp else None
  

def get_process_users_conf(config, item):
    get_user_udfs = rail.result('get_user_udfs')

    def get_all_permissionseturis(item):
        permissionsets = []
        replicon_permission_set = rail.result('get_all_permission_set')
        permissions = rail.find_first_by_attr_and_get_attr(config.PAY_PERIOD_MAPPER, "Pay Period Code", item['pay_period_code'], "Permissions", {})
        for k, v in permissions.items():
            permissionsets.append({
                'name': v,
                'uri': rail.find_first_by_attr_and_get_attr(replicon_permission_set,'displayText',v,'uri')
            })
        return permissionsets

    return {
        **item,
        **{
            'work_week': get_mapper_values(config, item['pay_period_code'], 'Work Week'),
            'work_week_uri': rail.find_first_by_attr_and_get_attr(config.WORKWEEK_MAPPER,'value', get_mapper_values(config, item['pay_period_code'], 'Work Week').split()[0].lower(),'uri') 
                if get_mapper_values(config, item['pay_period_code'], 'Work Week') else '',
            'todaysdate': get_today_date(config),
            'reim_currency_uri': get_user_udfs['reim_currency_uri'],
            'glc_uri': get_user_udfs['glc_uri'],
            'pay_period_code_uri': get_user_udfs['pay_period_code_uri'],
            'shift_schedule_name_uri': get_user_udfs['shift_schedule_name_uri'],
            'emp_status_uri': get_user_udfs['emp_status_uri'],
            'personal_action_code_uri': get_user_udfs['personal_action_code_uri'],
            'past_hire_date_uri': get_user_udfs['past_hire_date_uri'],
            'job_title_uri': get_user_udfs['job_title_uri'],
            'polaris_roles_uri': get_user_udfs['polaris_roles_uri'],
            'line_of_business_uri': get_user_udfs['line_of_business_uri'],
            'work_schedule_uri': get_user_udfs['work_schedule_uri'],
            'holiday_calander_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calendars'), 'displayText', get_mapper_values(config, item['pay_period_code'], 'Holiday Calendar'), 'uri'),
            'timezone': config.user_profile_timezone,
            'schedule_type': get_mapper_values(config, item['pay_period_code'], 'Schedule Type'),
            'pay_rule': get_mapper_values(config, item['pay_period_code'], 'Payrule'),
            'pay_rule_uri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_pay_rules"),'displayText',get_mapper_values(config, item['pay_period_code'], 'Payrule'),"uri")
                if get_mapper_values(config, item['pay_period_code'], 'Payrule') else null,
            'licences': get_licences_to_be_assigned(config, item['pay_period_code']),
            'timezoneuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_timezones'), 'displayText', config.user_profile_timezone, 'uri'),
            'permissionsetdetails': get_all_permissionseturis(item),
            'supervisor_permission_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'),'displayText',"Supervisor",'uri'),
            'country_name': rail.find_first_by_attr_and_get_attr(rail.result('get_updated_locations'), 'code', item['country'], 'name'),
            'reim_currency_value': rail.find_first_by_attr_and_get_attr(rail.result('get_reimburement_currency_values'),'displayText',item['home_currency'],'uri'),
            'glc_value': rail.find_first_by_attr_and_get_attr(rail.result('get_glc_values'),'displayText',item['glc'],'uri'),
            'pay_period_code_value': rail.find_first_by_attr_and_get_attr(rail.result('get_pay_period_code_values'),'displayText',item['pay_period_code'],'uri'),
            'get_emp_status_values': rail.find_first_by_attr_and_get_attr(rail.result('get_emp_status_values'),'displayText',item['status'],'uri'),
            'get_action_code_values': rail.find_first_by_attr_and_get_attr(rail.result('get_action_code_values'),'displayText',item['personal_action_code'],'uri'),
            'get_job_title_values': rail.find_first_by_attr_and_get_attr(rail.result('get_job_title_values'),'displayText',item['title_desc'],'uri'),
            'get_polaris_roles_values': rail.find_first_by_attr_and_get_attr(rail.result('get_polaris_roles_values'),'displayText',item['polaris_role'],'uri'),
            'get_line_of_business_values': rail.find_first_by_attr_and_get_attr(rail.result('get_line_of_business_values'),'displayText',item['hr_organization'],'uri'),
            'get_work_schedule_values': rail.find_first_by_attr_and_get_attr(rail.result('get_work_schedule_values'),'displayText',item['work_schedule'],'uri'),
            'department_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_updated_departments'),'name',item['org'],'uri'),
            'employee_type_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_updated_employee_types'),'name',get_employee_type_name(item['employee_type']),'uri'),
            'costcenteruri': rail.find_first_by_attr_and_get_attr(rail.result('get_pay_groups_data'),'displayText',get_pay_group_name(item['rate_type']),'uri'),
            'servicecenteruri': rail.find_first_by_attr_and_get_attr(rail.result('get_taxable_entities_data'),'displayText',taxable_entity_name(item),'uri'),
            'project_role_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_all_roles'), 'code', item['plc'],'uri'),
            'timesheetperiod':get_mapper_values(config, item['pay_period_code'], 'Timesheet Period'),
            'timesheet_period_uri':rail.find_first_by_attr_and_get_attr(rail.result('get_all_timesheet_period_list'), 'name', get_mapper_values(config, item['pay_period_code'], 'Timesheet Period'),'uri'),
            'timesheettemplateuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_sets"),'displayText',get_mapper_values(config, item['pay_period_code'], 'Timesheet Template'),"uri")
                if get_mapper_values(config, item['pay_period_code'], 'Timesheet Template') else null,
            'timesheettemplate': get_mapper_values(config, item['pay_period_code'], 'Timesheet Template'),
            'timesheetapprovalpath': get_mapper_values(config, item['pay_period_code'], 'Timesheet Template Approval Path'),
            'timesheetapprovalpathuri': rail.find_first_by_attr_and_get_attr(rail.result("get_timesheet_approval_paths"),'displayText',get_mapper_values(config, item['pay_period_code'], 'Timesheet Template Approval Path'),"uri") if get_mapper_values(config, item['pay_period_code'], 'Timesheet Template Approval Path') else null,
            # 'expensetemplate': get_mapper_values(config, item['pay_period_code'], 'Expense Template'),
            # 'expensetemplateuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_sets"),'displayText',get_mapper_values(config, item['pay_period_code'], 'Expense Template'),"uri")
            #     if get_mapper_values(config, item['pay_period_code'], 'Expense Template') else null,
            # 'expenseapprovalpath': get_mapper_values(config, item['pay_period_code'], 'Expense Approval Path'),
            # 'expenseapprovalpathuri': rail.find_first_by_attr_and_get_attr(rail.result("get_expense_approval_paths"),'displayText',get_mapper_values(config, item['pay_period_code'], 'Expense Approval Path'),"uri")
            #     if get_mapper_values(config, item['pay_period_code'], 'Expense Approval Path') else null,
            'timeofftemplate': get_mapper_values(config, item['pay_period_code'], 'Time off Template'),
            'timeofftemplateuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_sets"),'displayText',get_mapper_values(config, item['pay_period_code'], 'Time off Template'),"uri")
                if get_mapper_values(config, item['pay_period_code'], 'Time off Template') else null,
            'timeoffapprovalpath': get_mapper_values(config, item['pay_period_code'], 'Time off Approval Path'),
            'timeoffapprovalpathuri': rail.find_first_by_attr_and_get_attr(rail.result("get_timeoff_approval_paths"),'displayText',get_mapper_values(config, item['pay_period_code'], 'Time off Approval Path'),"uri")
                if get_mapper_values(config, item['pay_period_code'], 'Time off Approval Path') else null,
            'supervisor_log': rail.result('supervisor_processing_log'),
        }
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
    if not get_replicon_date(dag_run.conf['current_hire_date']):
        return False
    if dag_run.conf['termination_date']:
        if not  get_replicon_date(dag_run.conf['termination_date']):
            return False
    return True

def get_invalid_fields_message(dag_run):
    log=[]
    if not get_replicon_date(dag_run.conf['current_hire_date']):
        log.append('Invalid Date format for Start Date')
    if dag_run.conf['termination_date']:
        if not get_replicon_date(dag_run.conf['termination_date']):
            log.append('Invalid Date format for End Date')
    return rail.smartjoin_by_delim(log,";")

def get_process_new_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'user_log' : rail.result('create_user_log'),
            'timeoff_types_available': rail.result('get_all_time_off_types')['available_in_instance']
        }
    }

def get_enddate(enddate):
    if enddate:
        return datetime.strptime(enddate, DATE_FORMAT).strftime(EMAIL_DATE_FORMAT)
    return ''

def get_process_update_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'next_timesheet_start_date': get_updated_start_date(),
            'useruri': rail.result('get_user_data')[0]['userDetails']['uri'],
            'user_log' : rail.result('create_user_log'),
            'timeoff_types_available': rail.result('get_all_time_off_types')['available_in_instance'],
            'enddate': get_enddate(dag_run.conf['termination_date'])
        }
    }

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

def get_udfs(user_action, dag_run):
    udfs = []
    def add_udf_field_values(definitionuri, textvalue = null , dropdownuri = null, date = null):
        if definitionuri:
            if dropdownuri or date or textvalue:
                udfs.append({
                    "value": {
                        "customField": {
                            "uri": definitionuri,
                            "name": null
                        },
                        "text": textvalue,
                        "date": get_replicon_date(date) if date else null,
                        "dropDownOption": {
                            "uri": dropdownuri,
                            "name": null
                        } if dropdownuri else None,
                        "number": null
                    }
                })
    if user_action =='add_user':
        if dag_run.conf['home_currency']:
            add_udf_field_values(definitionuri = dag_run.conf['reim_currency_uri'], dropdownuri = dag_run.conf['reim_currency_value'])
        if dag_run.conf['glc']:
            add_udf_field_values(definitionuri = dag_run.conf['glc_uri'], dropdownuri = dag_run.conf['glc_value'])
        if dag_run.conf['status']:
            add_udf_field_values(definitionuri = dag_run.conf['emp_status_uri'], dropdownuri = dag_run.conf['get_emp_status_values'])
        if dag_run.conf['personal_action_code']:
            add_udf_field_values(definitionuri = dag_run.conf['personal_action_code_uri'], dropdownuri = dag_run.conf['get_action_code_values'])
        if dag_run.conf['detail_job_title']:
            add_udf_field_values(definitionuri = dag_run.conf['job_title_uri'], dropdownuri = dag_run.conf['get_job_title_values'])
        if dag_run.conf['past_hire_date']:
            add_udf_field_values(definitionuri = dag_run.conf['past_hire_date_uri'], date = dag_run.conf['past_hire_date'])
        if dag_run.conf['polaris_role']:
            add_udf_field_values(definitionuri = dag_run.conf['polaris_roles_uri'], dropdownuri = dag_run.conf['get_polaris_roles_values'])
        if dag_run.conf['hr_organization']:
            add_udf_field_values(definitionuri = dag_run.conf['line_of_business_uri'], dropdownuri = dag_run.conf['get_line_of_business_values'])
        if dag_run.conf['work_schedule']:
            add_udf_field_values(definitionuri = dag_run.conf['work_schedule_uri'], dropdownuri = dag_run.conf['get_work_schedule_values'])
        if dag_run.conf['pay_period_code']:
            add_udf_field_values(definitionuri = dag_run.conf['pay_period_code_uri'], dropdownuri = dag_run.conf['pay_period_code_value'])
        if dag_run.conf['shift_schedule_name']:
            add_udf_field_values(definitionuri = dag_run.conf['shift_schedule_name_uri'], textvalue = dag_run.conf['shift_schedule_name'])


    if user_action == 'update_user':
        custom_field_values = rail.result('get_user_data')[0]['userDetails']['customFieldValues']
        current_currency = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Reimbursement Currency', 'text')
        current_glc = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'GLC', 'text')
        current_status = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Employee Status', 'text')
        current_personal_act_cd = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Personnel Action Code', 'text')
        current_past_hire_dt = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Past Hire Date', 'text')
        current_detail_job_title = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Detail Job Title', 'text')
        current_polaris_roles = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Polaris Roles', 'text')
        current_hr_org = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Line of Business', 'text')
        current_work_schedule = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Work Schedule', 'text')
        current_shift_schedule_name = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Shift schedule Name', 'text')
        current_pay_period_code = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Pay Period Code', 'text')

        if dag_run.conf['home_currency'] and dag_run.conf['home_currency'] != current_currency:
            add_udf_field_values(definitionuri = dag_run.conf['reim_currency_uri'], dropdownuri = dag_run.conf['reim_currency_value'])
        if dag_run.conf['glc'] and dag_run.conf['glc'] != current_glc:
            add_udf_field_values(definitionuri = dag_run.conf['glc_uri'], dropdownuri = dag_run.conf['glc_value'])
        if dag_run.conf['status'] and dag_run.conf['status'] != current_status:
            add_udf_field_values(definitionuri = dag_run.conf['emp_status_uri'], dropdownuri = dag_run.conf['get_emp_status_values'])
        if dag_run.conf['personal_action_code'] and dag_run.conf['personal_action_code'] != current_personal_act_cd:
            add_udf_field_values(definitionuri = dag_run.conf['personal_action_code_uri'], dropdownuri = dag_run.conf['get_action_code_values'])
        if dag_run.conf['detail_job_title'] and dag_run.conf['detail_job_title'] != current_detail_job_title:
            add_udf_field_values(definitionuri = dag_run.conf['job_title_uri'], dropdownuri = dag_run.conf['get_job_title_values'])
        if dag_run.conf['past_hire_date'] and dag_run.conf['past_hire_date'] != current_past_hire_dt:
            add_udf_field_values(definitionuri = dag_run.conf['past_hire_date_uri'], date = dag_run.conf['past_hire_date'])
        if dag_run.conf['polaris_role'] and dag_run.conf['polaris_role'] != current_polaris_roles:
            add_udf_field_values(definitionuri = dag_run.conf['polaris_roles_uri'], dropdownuri = dag_run.conf['get_polaris_roles_values'])
        if dag_run.conf['hr_organization'] and dag_run.conf['hr_organization'] != current_hr_org:
            add_udf_field_values(definitionuri = dag_run.conf['line_of_business_uri'], dropdownuri = dag_run.conf['get_line_of_business_values'])
        if dag_run.conf['work_schedule'] and dag_run.conf['work_schedule'] != current_work_schedule:
            add_udf_field_values(definitionuri = dag_run.conf['work_schedule_uri'], dropdownuri = dag_run.conf['get_work_schedule_values'])
        if dag_run.conf['pay_period_code'] and dag_run.conf['pay_period_code'] != current_pay_period_code:
            add_udf_field_values(definitionuri = dag_run.conf['pay_period_code_uri'], dropdownuri = dag_run.conf['pay_period_code_value'])
        if dag_run.conf['shift_schedule_name'] and dag_run.conf['shift_schedule_name'] != current_shift_schedule_name:
            add_udf_field_values(definitionuri = dag_run.conf['shift_schedule_name_uri'], textvalue = dag_run.conf['shift_schedule_name'])
        
    return udfs

def get_timesheet_approvalpath(log, dag_run, user_action):
    if not dag_run.conf['timesheetapprovalpath']:
        return null
    if dag_run.conf['timesheetapprovalpath'] and not dag_run.conf['timesheetapprovalpathuri']:
        log.append(f"Timesheet Approval Path - {dag_run.conf['timesheetapprovalpath']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return {
                "value": {
                    "uri": dag_run.conf['timesheetapprovalpathuri'],
                    "name": null
                }
            }
    else:
        current_timesheet_approvalpath = rail.result('get_user_data')[0]['timesheetApprovalPath']
        if not current_timesheet_approvalpath or (current_timesheet_approvalpath and (
            dag_run.conf['timesheetapprovalpath'] != current_timesheet_approvalpath['displayText'])):
            return {
                "value": {
                    "uri": dag_run.conf['timesheetapprovalpathuri'],
                    "name": null
                }
            }
    return null

def get_expenses_approvalpath(log, dag_run, user_action):
    if not dag_run.conf['expenseapprovalpath']:
        return null
    if dag_run.conf['expenseapprovalpath'] and not dag_run.conf['expenseapprovalpathuri']:
        log.append(f"Timesheet Approval Path - {dag_run.conf['expenseapprovalpath']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return {
            "value": {
                "uri": dag_run.conf['expenseapprovalpathuri'],
                "name": null
            }
        }
    else:
        current_expense_approvalpath = rail.result('get_user_data')[0]['expenseApprovalPath']
        if not current_expense_approvalpath or (current_expense_approvalpath and (
            dag_run.conf['expenseapprovalpath'] != current_expense_approvalpath['displayText'])):
            return {
                "value": {
                    "uri": dag_run.conf['expenseapprovalpathuri'],
                    "name": null
                }
            }
    return null

def get_timeoff_approvalpath(log, dag_run, user_action):
    if not dag_run.conf['timeoffapprovalpath']:
        return null
    if dag_run.conf['timeoffapprovalpath'] and not dag_run.conf['timeoffapprovalpathuri']:
        log.append(f"Time Off Approval Path - {dag_run.conf['timeoffapprovalpath']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return {
                "value": {
                    "uri": dag_run.conf['timeoffapprovalpathuri'],
                    "name": null
                }
            }
    else:
        current_timeoff_approvalpath = rail.result('get_user_data')[0]['timeOffApprovalPath']
        if not current_timeoff_approvalpath or (current_timeoff_approvalpath and (
            dag_run.conf['timeoffapprovalpath'] != current_timeoff_approvalpath['displayText'])):
            return {
                "value": {
                    "uri": dag_run.conf['timeoffapprovalpathuri'],
                    "name": null
                }
            }
    return null

def get_timezone_uri(log, dag_run, user_action):
    if not dag_run.conf['timezone']:
        return null
    if dag_run.conf['timezone'] and not dag_run.conf['timezoneuri']:
        log.append(f"Timezone - {dag_run.conf['timezone']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return {
            "value": {
                "uri": dag_run.conf['timezoneuri'],
                "IANAName": null
            }
        }
    else:
        current_timezone = rail.result('get_user_data')[0]['timeZone']
        if not current_timezone or (current_timezone and (
            dag_run.conf['timezone'] != current_timezone['displayText'])):
            return {
                "value": {
                    "uri": dag_run.conf['timezoneuri'],
                    "IANAName": null
                }
            }
    return null

def get_timeoff_template_to_assign(log, dag_run, user_action):
    if not dag_run.conf['timeofftemplate']:
        return null
    if dag_run.conf['timeofftemplate'] and not dag_run.conf['timeofftemplateuri']:
        log.append(f"Time Off Template - {dag_run.conf['timeofftemplate']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return {
            "value": {
                "uri": dag_run.conf['timeofftemplateuri'],
                "name": null
            }
        }
    else:
        current_timeoff_template = rail.result('get_user_data')[0]['timeOffTemplate']
        if not current_timeoff_template or (current_timeoff_template and (
            dag_run.conf['timeofftemplate'] != current_timeoff_template['displayText'])):
            return {
                "value": {
                    "uri": dag_run.conf['timeofftemplateuri'],
                    "name": null
                }
            }
    return null

def get_effective_policy_set(todays_date, timesheet_schedule):
    """
    Get the effective policySet for a given effective date.

    Logic:
    - If effectiveDate is null, this is the initial policy (applicable from beginning)
    - If endDate is null, this is the current/latest policy (no end date)
    - Return the policy where: effectiveDate <= todays_date <= endDate

    Args:
        todays_date: Date dict with keys 'year', 'month', 'day' (e.g. from get_today_date())

    Returns:
        The policySet object if found, None otherwise
    """
    entry_date = get_dict_to_date(todays_date)
    for policy in timesheet_schedule:
        effective_date = get_dict_to_date(policy.get('effectiveDate'))
        end_date = get_dict_to_date(policy.get('endDate'))
        
        # Initial policy (no effectiveDate) with an endDate
        if effective_date is None and end_date is not None:
            if entry_date <= end_date:
                return policy.get('policySet')
        
        # Initial policy (no dates) - applies to everything
        elif effective_date is None and end_date is None:
            return policy.get('policySet')
        
        # Policy with effectiveDate but no endDate (current/latest)
        elif effective_date is not None and end_date is None:
            if entry_date >= effective_date:
                return policy.get('policySet')
        
        # Policy with both dates
        elif effective_date is not None and end_date is not None:
            if effective_date <= entry_date <= end_date:
                return policy.get('policySet')
    
    return None


def get_timesheet_template_to_assign(log, dag_run, user_action, effective_date):
    if not dag_run.conf['timesheettemplate']:
        return []
    if dag_run.conf['timesheettemplate'] and not dag_run.conf['timesheettemplateuri']:
        log.append(f"Timesheet Template - {dag_run.conf['timesheettemplate']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return [
            {
                "policyUri": "urn:replicon:policy:timesheet",
                "schedule": [
                    {
                        "policySetUri": dag_run.conf['timesheettemplateuri'],
                        "effectiveDate": null
                    }
                ]
            }
        ]
    else:
        custom_field_values = rail.result('get_user_data')[0]['userDetails']['customFieldValues']
        oncall_allowance = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'On call Allowance', 'text')
        if oncall_allowance and oncall_allowance == 'Yes':
            return []
        current_timesheet_template = rail.result('get_user_data')[0]['timesheetTemplate']
        if not current_timesheet_template:
            timesheet_schedule = rail.result('get_user_data')[0]['timesheetTemplateSchedule']
            current_timesheet_template = get_effective_policy_set(effective_date, timesheet_schedule)
        if not current_timesheet_template or (current_timesheet_template and (
            dag_run.conf['timesheettemplate'] != current_timesheet_template['displayText'])):
            return [
            {
                "policyUri": "urn:replicon:policy:timesheet",
                "schedule": [
                    {
                        "policySetUri": dag_run.conf['timesheettemplateuri'],
                        "effectiveDate": effective_date
                    }
                ]
            }
        ]
    return []

def get_expense_template_to_assign(log, dag_run, user_action):
    if not dag_run.conf['expensetemplate']:
        return null
    if dag_run.conf['expensetemplate'] and not dag_run.conf['expensetemplateuri']:
        log.append(f"Expense Template - {dag_run.conf['expensetemplate']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return {
            "value": {
                "uri": dag_run.conf['expensetemplateuri'],
                "name": null
            }
        }
    else:
        current_expense_template = rail.result('get_user_data')[0]['expenseTemplate']
        if not current_expense_template or (current_expense_template and (
            dag_run.conf['expensetemplate'] != current_expense_template['displayText'])):
            return {
                "value": {
                    "uri": dag_run.conf['expensetemplateuri'],
                    "name": null
                }
            }
    return null

def get_timeoff_types(timeoff_types_available, current_hire_date, user_action):
    resp = []
    if not timeoff_types_available:
        return resp
    if user_action == "add_user":
        for timeoff_type in timeoff_types_available:
            resp.append({
                    "timeOffType": {
                        "uri": timeoff_type['uri'],
                        "name": null
                    },
                    "isTimeOffAllowedAgainstThisTimeOffType": "true",
                    "applyDefaultTimeOffTypePolicy": "true",
                    "defaultTimeOffTypePolicyEffectiveDate": null,
                    "policySchedule": []
                }
            )
        if resp:
            resp = [{
                "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                "items": resp
            }]
    else:
        current_timeoff_types = rail.result('get_user_data')[0]['timeOffTypePolicySummary']['policiesByTimeOffType']
        current_start_date = rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']
        current_start_date = get_dict_to_date(current_start_date).strftime(INSTANCE_DATE_FORMAT)
        current_hire_date = datetime.strptime(current_hire_date, DATE_FORMAT).strftime(INSTANCE_DATE_FORMAT)
        current_timeoff_type_names = {}
        for current_timeoff_type in current_timeoff_types:
            current_timeoff_type_names[current_timeoff_type['timeOffType']['name']] = current_timeoff_type['timeOffType']['displayText']
        for timeoff_type in timeoff_types_available:
            if timeoff_type['name'] not in current_timeoff_type_names:
                resp.append({
                        "timeOffType": {
                            "uri": timeoff_type['uri'],
                            "name": null
                        },
                        "isTimeOffAllowedAgainstThisTimeOffType": "true",
                        "applyDefaultTimeOffTypePolicy": "true",
                        "defaultTimeOffTypePolicyEffectiveDate": null if current_hire_date == current_start_date else get_replicon_date(current_hire_date),
                        "policySchedule": []
                    }
                )
        if resp:
            resp = [{
                "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                "items": resp
            }]
    return resp

def get_location_schedule_to_assign(country_name, user_action, effective_date):
    if not country_name:
        return []
    if user_action == "add_user":
        return [
            {
                "dateRange": null,
                "item": {
                    "uri": null,
                    "parentUri": null,
                    "name": country_name
                }
            }
        ]
    else:
        current_location = rail.result('get_effective_user_groupmembership', 'location')
        if not current_location or (current_location and (
            country_name != current_location['displayText'])):
            return [
                {
                    "dateRange": {
                        "startDate": effective_date,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    } if current_location else null,
                    "item": {
                    "uri": null,
                    "parentUri": null,
                    "name": country_name
                    }
                }
            ]
    return []

def get_costcenter_schedule_to_assign(costcenteruri, user_action, effective_date):
    if not costcenteruri:
        return []
    if user_action == "add_user":
        return [
            {
                "dateRange": null,
                "item": {
                    "uri": costcenteruri,
                    "parentUri": null,
                    "name": null
                }
            }
        ]
    else:
        current_costcenter = rail.result('get_effective_user_groupmembership','costcenter')
        if not current_costcenter or (current_costcenter and (
            costcenteruri != current_costcenter['uri'])):
            return [
                {
                    "dateRange": {
                        "startDate": effective_date,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    } if current_costcenter else null,
                    "item": {
                        "uri": costcenteruri,
                        "parentUri": null,
                        "name": null
                    }
                }
            ]
    return []

def get_servicecenter_schedule_to_assign(servicecenteruri, user_action, effective_date):
    if not servicecenteruri:
        return []
    if user_action == "add_user":
        return [
            {
                "dateRange": null,
                "item": {
                    "uri": servicecenteruri,
                    "parentUri": null,
                    "name": null
                }
            }
        ]
    else:
        current_servicecenter = rail.result('get_effective_user_groupmembership','servicecenter')
        if not current_servicecenter or (current_servicecenter and (
            servicecenteruri != current_servicecenter['uri'])):
            return [
                {
                    "dateRange": {
                        "startDate": effective_date,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    } if current_servicecenter else null,
                    "item": {
                        "uri": servicecenteruri,
                        "parentUri": null,
                        "name": null
                    }
                }
            ]
    return []

def get_department_schedule_to_assign(departmenturi, user_action, effective_date):
    if not departmenturi:
        return []
    if user_action == "add_user":
        return [
            {
                "dateRange": null,
                "item": {
                    "uri": departmenturi,
                    "parentUri": null,
                    "name": null
                }
            }
        ]
    else:
        current_department = rail.result('get_effective_user_groupmembership','department')
        if not current_department or (current_department and (
            departmenturi != current_department['uri'])):
            return [
                {
                    "dateRange": {
                        "startDate": effective_date,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    } if current_department else null,
                    "item": {
                        "uri": departmenturi,
                        "parentUri": null,
                        "name": null
                    }
                }
            ]
    return []

def get_employeetype_schedule_to_assign(employeetypeuri, user_action, effective_date):
    if not employeetypeuri:
        return []
    if user_action == "add_user":
        return [
            {
                "dateRange": null,
                "item": {
                    "uri": employeetypeuri,
                    "parentUri": null,
                    "name": null
                }
            }
        ]
    else:
        current_employeetype = rail.result('get_effective_user_groupmembership','employeetype')
        if not current_employeetype or (current_employeetype and (
            employeetypeuri != current_employeetype['uri'])):
            return [
                {
                    "dateRange": {
                        "startDate": effective_date,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    } if current_employeetype else null,
                    "item": {
                        "uri": employeetypeuri,
                        "parentUri": null,
                        "name": null
                    }
                }
            ]
    return []

def get_permissionsets_to_assign(permissionsets, user_action):
    resp = []
    if not permissionsets:
        return resp
    if user_action == "add_user":
        for permissionset in permissionsets:
            resp.append({
                    "permissionSetPolicy": {
                    "uri": permissionset['uri'],
                    "name": null
                    },
                    "groupAccessFilter": null
                }
            )
        if resp:
            resp = [{
                "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                "items": resp
            }]
    else:
        current_permissionsets = rail.result('get_user_data')[0]['permissionSets']
        current_permissionsets_names = {}
        for current_permissionset in current_permissionsets:
            current_permissionsets_names[current_permissionset['displayText']] = current_permissionset['uri']
        for permissionset in permissionsets:
            if permissionset['name'] not in current_permissionsets_names:
                resp.append({
                        "permissionSetPolicy": {
                        "uri": permissionset['uri'],
                        "name": null
                        },
                        "groupAccessFilter": null
                    }
                )
        if resp:
            resp = [{
                "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                "items": resp
            }]
    return resp

def get_timesheet_period_to_assign(timesheetperioduri, user_action, effective_date):
    if not timesheetperioduri:
        return []
    if user_action == "add_user":
        return [
            {
                "dateRange": null,
                "item": {
                    "uri": timesheetperioduri,
                    "name": null
                }
            }
        ]
    # else: # commenting this for now as it is not in scope with CR V1.10 but for the future CRs keeping the code here
    #     current_timesheetperiod = rail.result('get_user_data')[0]['timesheetPeriodSchedule']
    #     if not current_timesheetperiod or (current_timesheetperiod and (
    #         timesheetperioduri != current_timesheetperiod[-1]['timesheetPeriod']['uri'])):
    #         return [
    #             {
    #                 "dateRange": {
    #                     "startDate": get_replicon_date(effective_date),
    #                     "endDate": null,
    #                     "relativeDateRangeUri": null,
    #                     "relativeDateRangeAsOfDate": null
    #                 } if current_timesheetperiod else null,
    #                 "item": {
    #                     "uri": timesheetperioduri,
    #                     "name": null
    #                 }
    #             }
    #         ]
    return []

def get_schedule_type_to_assign(schedule_type, user_action, effective_date=null):
    if not schedule_type:
        return []
    if schedule_type == "Shift":
        item = {
          "scheduleTypeUri": "urn:replicon:schedule-type:shift",
          "officeSchedule": null
        }
    else:
        item = {
                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                "officeSchedule": {
                    "officeScheduleUri": null,
                    "name": schedule_type
                }
            }
    if user_action == "add_user":
        return [
            {
                "dateRange": null,
                "item": item
            }
        ]
    else:
        current_schedule_type = rail.result('get_user_data')[0]['schedulePolicies']
        if current_schedule_type and (current_schedule_type[-1]['scheduleTypeUri'] == "urn:replicon:schedule-type:shift" and schedule_type == "Shift"):
            return []
        if not current_schedule_type or not current_schedule_type[-1]['officeSchedule'] or (current_schedule_type and (
            schedule_type != current_schedule_type[-1]['officeSchedule']['displayText'])):
            return [
                {
                    "dateRange": {
                        "startDate": get_replicon_date(effective_date),
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    } if current_schedule_type and effective_date else null,
                    "item": item
                }
            ]
    return []

def get_project_role_to_assign(projectroleuri, user_action, effective_date):
    if not projectroleuri:
        return []
    if user_action == "add_user":
        return [
            {
            "dateRange": null,
            "item": {
                "projectRole": {
                    "uri": projectroleuri,
                    "name": null
                },
                "isPrimary": "true"
                }
            }
        ]
    else:
        current_schedule_type = rail.result('get_user_assigned_role_from_replicon')
        if not current_schedule_type or (current_schedule_type and (
            projectroleuri != current_schedule_type[0]['schedule'][0]['projectRoles'][0]['projectRole']['uri'])):
            return [
                {
                    "dateRange": {
                        "startDate": get_replicon_date(effective_date),
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    } if current_schedule_type else null,
                    "item": {
                        "projectRole": {
                            "uri": projectroleuri,
                            "name": null
                        },
                        "isPrimary": "true"
                    }
                }
            ]
    return []

def get_holiday_calendar_to_assign(holiday_calendar_uri, user_action, effective_date):
    if not holiday_calendar_uri:
        return []
    if user_action == "add_user":
        return {
            "value": {
                "uri": holiday_calendar_uri,
                "name": null
            }
        }
    # else: # commenting this for now as it is not in scope with CR V1.10 but for the future CRs keeping the code here
    #     current_holiday_calendar = rail.result('get_user_data')[0]['holidayCalendar']
    #     if not current_holiday_calendar or (current_holiday_calendar and (
    #         holiday_calendar_uri != current_holiday_calendar['uri'])):
    #         return {
    #             "value": {
    #                 "uri": holiday_calendar_uri,
    #                 "name": null
    #             }
    #         }
    return []

def get_pay_rule_to_assign(pay_rule, pay_rule_uri, user_action, effective_date=null):
    if not pay_rule_uri:
        return []
    if user_action == "add_user":
        return [
            {
                "dateRange": null,
                "item": {
                    "uri": pay_rule_uri,
                    "name": null
                }
            }
        ]
    else:
        current_pay_rule = rail.result('get_user_data')[0]['payRuleScriptSchedule']
        if not current_pay_rule or (current_pay_rule and (
            pay_rule != current_pay_rule[-1]['payRuleScript']['displayText'])):
            return [
                {
                    "dateRange": {
                        "startDate": get_replicon_date(effective_date),
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    } if current_pay_rule and effective_date else null,
                    "item": {
                        "uri": pay_rule_uri,
                        "name": null
                    }
                }
            ]
    return []

def get_workweek_start_day(log, dag_run, user_action):
    if not dag_run.conf['work_week']:
        return null
    if dag_run.conf['work_week'] and not dag_run.conf['work_week_uri']:
        log.append(f"Work Week - {dag_run.conf['work_week']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return {
            "value": {
                "uri": dag_run.conf['work_week_uri']
            }
        }
    else:
        current_workweek = rail.result('get_user_data')[0]['userDetails']['workWeekStartDay']
        if not current_workweek or (current_workweek and (
            dag_run.conf['work_week_uri'] != current_workweek['uri'])):
            return {
                "value": {
                    "uri": dag_run.conf['work_week_uri']
                }
            }
    return null

def get_create_update_user_payload(config, dag_run, user_action):
    log=[]
    put_user_payload = {
        "target": {
            "uri": dag_run.conf['useruri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        } if user_action == "update_user" else null,
        "template": null,
        "modifications": {
            "firstName": {
                "value": dag_run.conf['first_name']
            },
            "lastName": {
                "value": dag_run.conf['last_name']
            },
            "loginName": {
                "value": dag_run.conf['email_id']
            } if user_action == "add_user" else null,
            "displayName": {
                "value": dag_run.conf['display_name']
            },
            "emailAddress": {
                "value": dag_run.conf['email_id']
            },
            "employeeId": {
                "value": dag_run.conf['empl_id']
            },
            "employmentDateRange": {
                "value": {
                    "startDate": get_replicon_date(dag_run.conf['current_hire_date']),
                    "endDate": get_replicon_date(dag_run.conf['termination_date']) if dag_run.conf['termination_date'] and user_action == "update_user" else null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": "true" if dag_run.conf['status'] == "Active" else "false",
                    },
                    "forcePasswordChange": null,
                    "ssoName": {
                        "value": dag_run.conf['email_id']
                    },
                    "ssoNameModificationOptionUri": null,
                    "password": null,
                    "authenticationProviders": [],
                    "emailMFAResendVerificationEmail": null,
                    "emailMFATryAddMethodFromUsersEmail": null,
                    "isMFAMethodRequired": null,
                    "clearIsLockedOut": null
                },
            } if user_action == "add_user" else None,
            "timesheetApprovalPath": get_timesheet_approvalpath(log,dag_run, user_action),
            "timeEntryApprovalPath": null,
            "workAuthorizationApprovalPath": null,
            "timeoffApprovalPath": get_timeoff_approvalpath(log,dag_run, user_action),
            "timeOffBalancePayoutApprovalPath": null,
            "defaultActivity": null,
            # "expenseApprovalPath": get_expenses_approvalpath(log,dag_run, "add_user") if user_action == "add_user" else get_expenses_approvalpath(log,dag_run, "update_user"),
            "timeZone": get_timezone_uri(log, dag_run, user_action),
            "workWeekStartDay": get_workweek_start_day(log, dag_run, user_action),
            "defaultBillingRate": null,
            "userPreferences": null,
            "formattings": null,
            "notificationPreferences": get_notification_settings(),
            "timesheetTemplate": null,
            "timeoffTemplate": get_timeoff_template_to_assign(log, dag_run, "add_user") if user_action == "add_user" else get_timeoff_template_to_assign(log, dag_run, "add_user"),
            "timeOffCalendarVisibility": null,
            # "expenseTemplate": get_expense_template_to_assign(log, dag_run, "add_user") if user_action == "add_user" else get_expense_template_to_assign(log, dag_run, "add_user"),
            "workAuthorizationTemplate": null,
            "punchEntryPolicy": null,
            "holidayCalendar": get_holiday_calendar_to_assign(dag_run.conf['holiday_calander_uri'], "add_user", dag_run.conf['todaysdate'])  if user_action == "add_user" else [],
            "extensionFields": [],
            "customFields": get_udfs(user_action, dag_run),
            "products": [],
            "skills": [],
            "activities": [],
            "policySets": [],
            "permissionSets": get_permissionsets_to_assign(dag_run.conf['permissionsetdetails'], user_action),
            "bankedTimePolicies": [],
            "timeOffTypes": get_timeoff_types(dag_run.conf['timeoff_types_available'], dag_run.conf['current_hire_date'], user_action),
            "locationSchedule": get_location_schedule_to_assign(dag_run.conf['country_name'], "add_user", dag_run.conf['todaysdate']) if user_action == "add_user" else get_location_schedule_to_assign(dag_run.conf['country_name'], "update_user", dag_run.conf['todaysdate']),
            "divisionSchedule": [],
            "costCenterSchedule": get_costcenter_schedule_to_assign(dag_run.conf['costcenteruri'], user_action, dag_run.conf['todaysdate']),
            "serviceCenterSchedule": get_servicecenter_schedule_to_assign(dag_run.conf['servicecenteruri'], user_action, dag_run.conf['todaysdate']),
            "departmentGroupSchedule": get_department_schedule_to_assign(dag_run.conf['department_uri'], user_action, dag_run.conf['todaysdate']),
            "employeeTypeGroupSchedule": get_employeetype_schedule_to_assign(dag_run.conf['employee_type_uri'], user_action, dag_run.conf['todaysdate']),
            "supervisorSchedule": [],
            "timesheetPeriodSchedule": get_timesheet_period_to_assign(dag_run.conf['timesheet_period_uri'], user_action, dag_run.conf['todaysdate'])  if user_action == "add_user" else [],
            "holidayCalendarSchedule": [],
            "scheduleTypeSchedule": get_schedule_type_to_assign(dag_run.conf['schedule_type'], user_action, dag_run.conf.get('next_timesheet_start_date')),
            "payRuleSchedule": get_pay_rule_to_assign(dag_run.conf['pay_rule'], dag_run.conf['pay_rule_uri'], user_action, dag_run.conf.get('next_timesheet_start_date')),
            "placeSchedule": [],
            "payRateSchedule": [],
            "projectRoleSchedule": get_project_role_to_assign(dag_run.conf['project_role_uri'], user_action, dag_run.conf['todaysdate']),
            "costNormalizationRuleSchedule": [],
            "hourlyRatesSchedule": [
            {
                "dateRange": null,
                "item": {
                "hourlyRate": {
                    "amount": round((float(dag_run.conf['hourly_cost'])), 2),
                    "currency": {
                        "uri": null,
                        "name": null,
                        "symbol": dag_run.conf['home_currency']
                    }
                }
                }
            }
            ] if user_action == "add_user" and dag_run.conf.get('hourly_cost') else [],
            "substituteUserSchedule": [],
            "policySetsScheduleToApply": get_timesheet_template_to_assign(log, dag_run, user_action, dag_run.conf['todaysdate']),
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

    rail.set_result(key="exception_logs",val= log)

    return put_user_payload

def validate_enddate(dag_run):
    if dag_run.conf['current_hire_date'] and dag_run.conf['termination_date']:
        return datetime.strptime(dag_run.conf['termination_date'], DATE_FORMAT) > datetime.strptime(dag_run.conf['current_hire_date'], DATE_FORMAT)
    return False

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


def validate_supervisor_changed():
    if not rail.result('get_effective_supervisor_of_user'):
        return True
    if rail.result('search_supervisor_in_replicon') and rail.result('get_effective_supervisor_of_user') and \
        rail.result('search_supervisor_in_replicon')['loginname'] == rail.result('get_effective_supervisor_of_user')['supervisor']['user']['loginName']:
        return False
    return True

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
    uri = rail.find_first_by_attr_and_get_attr(all_permissionsets, 'name', 'Supervisor DPS', 'uri')
    return uri

def get_term_date(dag_run, current_empl_daterange, time_zone):
    dt = get_current_date(time_zone)
    if dag_run.conf['termination_date']:
        dt = datetime.strptime(dag_run.conf['termination_date'], DATE_FORMAT)
    elif current_empl_daterange.get("endDate"):
        dt = parse_date_json(current_empl_daterange["endDate"])
    return dt

def get_update_existing_user_profile(dag_run, time_zone, user_details, update_existing_profile):
    user_uri = user_details['uri']
    current_hire_date = get_replicon_date(dag_run.conf['current_hire_date'])
    return {
        "target": {
            "uri": user_uri
        },
        "modifications": {
            **update_existing_profile
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_customfield_dropdown_option(existing_dropdowns_task_id, list_to_add_key):
    existing_dropdowns_list = rail.result(existing_dropdowns_task_id)
    dropdowns_list_add = rail.result('get_udf_values_to_add').get(list_to_add_key, [])
    final_dropdown_list = list(map(lambda x: {
        'target': {
            'uri': x['uri'],
            'name': x['displayText']
        },
        'name': x['displayText'],
        'isEnabled': x['isEnabled']
    }, existing_dropdowns_list)) if existing_dropdowns_list else []

    for udf_name in dropdowns_list_add:
        final_dropdown_list.append({
        'name': udf_name,
        'isEnabled': True
    })
    return final_dropdown_list

def get_notification_settings():
    notification_preferences = []
    for notification in ALL_NOTIFICATIONS:
        if notification in ENABLE_NOTIFICATIONS:
            notification_preferences.append({
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver",
                "objectTypeUri": f"urn:replicon:object-type:{notification}"
            })
            continue
        notification_preferences.append({
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver",
                "objectTypeUri": f"urn:replicon:object-type:{notification}"
            })
    return {
        "value": {
            "notificationDeliveryPreferences": notification_preferences,
            "sharedDeliveryPreferenceOptionUris": [
                "urn:replicon:user-shared-delivery-preference-option:always-deliver"
            ]
        }
    }

def get_timesheet_details(dag_run,config):
    user_data = rail.result('get_user_data')[0]['userDetails']
    current_hire_date = get_replicon_date(dag_run.conf['current_hire_date'])  # Converts to {'year': 2025, 'month': 7, 'day': 10}
    today_date = get_today_date(config)  # Already in {'year': 2025, 'month': 8, 'day': 28} format
    
    # Convert to date objects for comparison
    hire_date_obj = date(current_hire_date['year'], current_hire_date['month'], current_hire_date['day'])
    today_date_obj = date(today_date['year'], today_date['month'], today_date['day'])
    
    # If hire date is in the future, use hire date, otherwise use today
    effective_date = current_hire_date if hire_date_obj > today_date_obj else today_date
    
    return {
        "userUri": user_data['uri'],
        "asOfDate": effective_date
    }
