from datetime import datetime
import json
import rail
from rail.lib.ecid import get_dagrun_ecid

def effective_dateformat_payload(effective_date):
    return {
        "year": effective_date.year,
        "month": effective_date.month,
        "day": effective_date.day
    }

def get_datetime_obj(effectivedate):
    effective_date = datetime.strptime(effectivedate, '%Y-%m-%d')
    return {
        "year": effective_date.year,
        "month": effective_date.month,
        "day": effective_date.day
    }

def update_emp_date_for_disableuser(dag_run):
    hiredate = datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d')
    terminationdate = datetime.strptime(dag_run.conf['terminationdate'], '%Y-%m-%d')
    return {
        "userUri": dag_run.conf['useruri'],
        "dateRange": {
            "startDate": effective_dateformat_payload(hiredate),
            "endDate": effective_dateformat_payload(terminationdate)
        }
    }

def get_balancesummary_foraccount(dag_run):
    effective_date = datetime.strptime(str(datetime.now().date()), '%Y-%m-%d')
    return {
        "account": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('foreach_policiesby_timeofftype')['timeOffType']['uri']
        },
        "asOfDate": effective_dateformat_payload(effective_date)
    }

def put_remainig_balance_for_payout_parameter(dag_run):
    effective_date = datetime.now().date()
    return {
        'timeoffuri':rail.result('foreach_policiesby_timeofftype')['timeOffType']['uri'],
        'useruri':dag_run.conf['useruri'],
        'terminationdate':effective_date.day + '/' + effective_date.month + '/' + effective_date.year,
        'startingbalancesettouri':rail.result('get_all_scripts'),
        'balance': 0
    }

def put_remainig_balance_for_payout_parameter_0(dag_run):
    effective_date = datetime.strptime(dag_run.conf['terminationdate'], '%Y-%m-%d')
    return {
        'timeoffuri':rail.result('foreach_policiesby_timeofftype')['timeOffType']['uri'],
        'useruri':dag_run.conf['useruri'],
        'terminationdate':effective_date.day + '/' + effective_date.month + '/' + effective_date.year,
        'startingbalancesettouri':rail.result('get_all_scripts'),
        'balance': 0
    }

def put_remainig_balance_for_payout_parameter_annual(dag_run):
    effective_date = datetime.strptime(dag_run.conf['terminationdate'], '%Y-%m-%d')
    return {
        'timeoffuri':rail.result('foreach_policiesby_timeofftype')['timeOffType']['uri'],
        'useruri':dag_run.conf['useruri'],
        'terminationdate':effective_date.day + '/' + effective_date.month + '/' + effective_date.year,
        'startingbalancesettouri':rail.result('get_all_scripts'),
        'balance': int(rail.result('get_balance_summary_foraccount'))
    }

def get_put_timeoffpolicywithinitialbalance(dag_run):

    effective_date = datetime.strptime(dag_run.conf['terminationdate'], '%d/%m/%Y')

    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri']
        },
        "policySetScheduleEntries": rail.result('past_policyset_schedule') +
        [{
            "effectiveDate": {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            },
            "description": f"Effective on {effective_date.month}/{effective_date.day}/{effective_date.year}",
            "policySet": {
                "timeOffBalanceEventScripts": [
                    {
                        "scriptTarget": {
                            "uri": dag_run.conf['startingbalancesettouri']
                        },
                        "additionalParameters": [
                            {
                                "keyUri": "urn:replicon:script-key:parameter:amount",
                                "value": {
                                    "number": str(float(dag_run.conf['balance']))
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

def get_user_timeoff_policy_payload(dag_run):
    return{
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri']
        },
        "policySetScheduleEntries": json.loads(rail.result('get_default_time_off_type_policy_schedule_for_user'))
    }

def get_timesheetperiod_val_92(dag_run):
    hiredate = datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d')
    return [
        {
            "timesheetPeriod":{
                "uri": None,
                "name": 'Monthly'
            },
            "effectiveDate": effective_dateformat_payload(hiredate)
        }
    ]

def get_timesheetperiod_val_109(dag_run):
    hiredate = datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d')
    return [
        {
            "timesheetPeriod":{
                "uri": None,
                "name": 'Korea_Weekly Timesheet'
            },
            "effectiveDate": effective_dateformat_payload(hiredate)
        }
    ]

def create_user_payload(dag_run):
    hiredate = datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d')
    return {
        "user": {
            "target": {
                "loginName": dag_run.conf['userid']
            },
            "firstname": dag_run.conf['firstname'],
            "lastname": dag_run.conf['lastname'],
            "emailAddress": dag_run.conf['emailaddress'],
            "employeeId": dag_run.conf['workerreferenceemployeeid'],
            "schedulePolicySchedule": rail.result('get_schedule_variable')['value'],
            "workWeekStartDayUri": rail.result('usermappings_mapper')['workweek'],
            "employmentDateRange": {
                "startDate": effective_dateformat_payload(hiredate)
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['userid'],
                "SSOName": dag_run.conf['userid']
            },
            "holidayCalendar": rail.result('get_holidaycalendar_variable')['value'],
            "permissionSets": [
                {
                    "uri":rail.result('get_all_permissionsets')['basic_user_with_report_uri']
                }
            ],
            "policySets": rail.result('get_policyset_variable')['value'],
            "timesheetApprovalPath": rail.result('get_timesheetapprovalpath_variable')['value'],
            "timeOffApprovalPath": rail.result('get_timeoffapprovalpath_variable')['value'],
            "timeZone": {
                "IANAName": rail.result('usermappings_mapper')['timezone']
            },
            "divisionSchedule": rail.result('get_legalentity_division_variable')['value'],
            "costCenterSchedule":rail.result('get_costcenter_variable')['value'],
            "serviceCenterSchedule": rail.result('get_paygrp_srvcenter_variable')['value'],

            "departmentGroupSchedule": [
                {
                    "departmentGroup":{
                        "uri": dag_run.conf['departmentgroupuri']
                    }
                }
            ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup":{
                        "uri": rail.result('get_required_employeetype_uri')
                    }
                }
            ],
            "timesheetPeriodSchedule": rail.result('get_timesheetperiod_variable')['value'],
            "payRuleScriptSchedule": rail.result('get_payrule_variable')['value']
        }
    }

def trigger_timeoff_addnew_user(dag_run):
    return {
        "loginname": dag_run.conf['userid'],
        "startdate": dag_run.conf['hiredate'],
        "useruri": rail.result('create_user')['uri'],
        "terminationdate": dag_run.conf['terminationdate'],
        "active": dag_run.conf['active'],
        "timeofftypes": rail.result('usermappings_mapper')['timeoffs'],
        "continous_service_date": dag_run.conf['continous_service_date'] if dag_run.conf['continous_service_date'] else '',
        "rehire": 'add'
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
                    "uri": rail.result('getdata_sup_emp_grp_dept_grp')['rows'][0]['cells'][2]['uri']
                }
            }
        }
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

def payrule_update_payload(dag_run):
    return {
        "user": {
            "uri": dag_run.conf['useruri']
        },
        "modifications": {
            "payRulesScheduleModifications": {
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": rail.result('get_req_payrule_script')
                        },
                        "effectiveDate": get_datetime_obj(rail.result('get_startdate_of_next_timesheet')) if rail.result(
                            'get_startdate_of_next_timesheet') else get_datetime_obj(str(datetime.now().date()))
                    }
                ]
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def update_servicecenter_payload(dag_run):
    return {
        "user": {
            "uri" : dag_run.conf['useruri']
        },
        "modifications": {
            "serviceCenterScheduleToApply": {
                "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementServiceCenterSchedule": [],
                "updateServiceCenterScheduleOverDateRange": {
                    "replacementServiceCenterScheduleEntries": [
                        {
                            "serviceCenter": {
                                "uri": dag_run.conf['paygroupuri']
                            },
                            "effectiveDate": effective_dateformat_payload(datetime.now())
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_update_costcenter_param(dag_run):
    return {
        "user": {
            "uri":dag_run.conf['useruri']
        },
        "modifications": {
            "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementCostCenterSchedule": [],
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {
                                "uri": dag_run.conf['costcenteruri']
                            },
                            "effectiveDate": get_datetime_obj(dag_run.conf['worker_cc_change_date']) if dag_run.conf['worker_cc_change_date'] \
                                else effective_dateformat_payload(datetime.now())
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def apply_user_modifications_division(dag_run):
    return {
        "user": {
            "uri": dag_run.conmf['useruri']
        },
        "modifications": {
            "divisionScheduleToApply": {
                "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDivisionSchedule": [],
                "updateDivisionScheduleOverDateRange": {
                    "replacementDivisionScheduleEntries": [{
                        "division": {
                            "uri": dag_run.conf['legalentityuri']
                        },
                        "effectiveDate": effective_dateformat_payload(datetime.now())
                    }]
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
                            "effectiveDate": get_datetime_obj(dag_run.conf['CF_LRV_Location_Change_Effective_Date']) \
                                if dag_run.conf['CF_LRV_Location_Change_Effective_Date'] else effective_dateformat_payload(datetime.now())
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
            "schedulePolicyToApply":{
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "updateScheduleOverDateRange":{
                    "replacementScheduleEntries":[{
                        "schedulePolicy":{
                            "officeScheduleUri": None if rail.result('usermappings_mapper')['schedule'] == 'Shift' else rail.result('get_req_schedule_script'),
                            "officeSchedule":{
                                "officeScheduleUri":None if rail.result(
                                    'usermappings_mapper')['schedule'] == 'Shift' else rail.result('get_req_schedule_script'),
                            },
                             "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate":get_datetime_obj(dag_run.conf['work_shift_change_effective_date']) \
                                if dag_run.conf['work_shift_change_effective_date'] else effective_dateformat_payload(datetime.now())
                    }]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def trigger_updateuser_timeoff(dag_run):
    strt_date = rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']
    return {
        "hiredate" : dag_run.conf['hiredate'],
        "terminationdate" : dag_run.conf['terminationdate'],
        "active": dag_run.conf['active'],
        "rehire" :dag_run.conf['rehireupdate'],
        "timeofftypes": rail.result('usermappings_mapper')['timeoffs'],
        "continous_service_date": dag_run.conf['continous_service_date'],
        "timeoff_service_date": dag_run.conf['timeoff_service_date'] if dag_run.conf['timeoff_service_date'] else dag_run.conf['continous_service_date'],
        "old_startdate":str(strt_date['year']) + '-' +  str(strt_date['month']) + '-' + str(strt_date['day']),
        "useruri": dag_run.conf['useruri'],
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
                "startDate": get_datetime_obj(dag_run.conf['sup_change_effective_date'])
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
                    "uri":rail.result('get_all_permissionsets')['supervisor']
                }
            ],
            "departmentGroupSchedule": [
                {
                    "departmentGroup":{
                        "name": "Momentive",
                    }
                }
            ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup":{
                        "name": "Foreign Supervisors",
                    }
                }
            ]
        }
    }

def add_to_policy_38():
    yr = str(int(datetime.now().year) + 1)
    if float(rail.result('get_years_of_service')) > 0.99 :
        yr = str(datetime.now().year)

    return {
        'description': "Policy effective from 01/01/" + yr,
        'effectiveDate': {
            'day': '01',
            'month': '01',
            'year': yr
        },
        'policySet': rail.result('log_new_policy_to_assign_36')
    }

def add_to_policy_newuser_93():
    yr = str(int(datetime.now().year) + 1)
    if float(rail.result('get_years_of_service')) > 0.99 :
        yr = str(datetime.now().year)

    return {
        'description': "Policy effective from 01/01/" + yr,
        'effectiveDate': {
            'day': '01',
            'month': '01',
            'year': yr
        },
        'policySet': rail.result('log_new_policy_to_assign_91')
    }

def add_to_policy_68():
    return {
        'description': "Policy effective from 01/01/" + str(datetime.now().year),
        'effectiveDate': {
            'day': '01',
            'month': '01',
            'year': str(datetime.now().year)
        },
        'policySet': rail.result('log_new_policy_to_assign_62')
    }

def add_to_policy_72():
    return {
        'description': "Policy effective from 01/01/" + str(int(datetime.now().year) + 2),
        'effectiveDate': {
            'day': '01',
            'month': '01',
            'year': str(int(datetime.now().year) + 2)
        },
        'policySet': rail.result('log_3rd_yr_policyset')
    }

def add_to_policy_81(dag_run):
    return {
        'description': "Policy effective from" + dag_run.conf['startdate'],
        'effectiveDate': get_datetime_obj(dag_run.conf['startdate']),
        'policySet': rail.result('log_new_policy_to_assign_79')
    }

def add_to_policy_87(dag_run):
    return {
        'description': "Policy effective from" + dag_run.conf['startdate'],
        'effectiveDate': get_datetime_obj(dag_run.conf['startdate']),
        'policySet': rail.result('log_new_policy_to_assign_85')
    }

def add_to_policy_92():
    return {
        'description': "Policy effective from 01/01/" + str(int(datetime.now().year) + 1),
        'effectiveDate': {
            'day': '01',
            'month': '01',
            'year': str(int(datetime.now().year) + 1)
        },
        'policySet': rail.result('log_new_policy_to_assign_90')
    }

def add_to_policy_97():
    return {
        'description': "Policy effective from 01/01/" + str(int(datetime.now().year) + 2),
        'effectiveDate': {
            'day': '01',
            'month': '01',
            'year': str(int(datetime.now().year) + 2)
        },
        'policySet': rail.result('log_3rd_yr_policyset_95')
    }

def add_to_policy_newuser_33(dag_run):
    return {
        'description': "Policy effective from" + dag_run.conf['startdate'],
        'effectiveDate': get_datetime_obj(dag_run.conf['startdate']),
        'policySet': rail.result('log_new_policy_to_assign_newuser_31')
    }

def add_to_policy_newuser_60(dag_run):
    return {
        'description': "Policy effective from" + dag_run.conf['startdate'],
        'effectiveDate': get_datetime_obj(dag_run.conf['startdate']),
        'policySet': rail.result('log_new_policy_to_assign_newuser_58')
    }


def add_to_noaccrualpolicy_94():
    return {
        'policySet':{
            'timeOffBalanceEventScripts':{
                'additionalParameters':{
                    'keyUri':'urn:replicon:script-key:parameter:amount',
                    'value':{
                        'number':0
                    }
                },
                'script':{
                    'description':'Set initial balance for the first day of a policy',
                    'name':'Starting Balance Set To',
                    'uri': rail.result('get_timeoffbalance_event_script_administration_service')['startring_balance']
                }
            },
            'timeOffValidationScripts':{
                'additionalParameters':{
                    'keyUri':'urn:replicon:script-key:parameter:maximum-overdraw',
                    'value':{
                        'number':0
                    }
                },
                'script':{
                    'description':"Do not allow the user's time off balance to go below the overdraw threshold",
                    'name':'Prevent balance overdraw',
                    'uri': rail.result('get_all_scripts_timeOff_validation_script')['prevent_bal']
                }
            }
        }
    }

def user_import_data(item):
    return [
        item['User_ID'] if item['User_ID'] else '',
        item['Worker_Reference_Employee_ID'] if item['Worker_Reference_Employee_ID'] else '',
        item['Email_Address'] if item['Email_Address'] else '',
        item['First_Name'] if item['First_Name'] else '',
        item['Last_Name'] if item['Last_Name'] else '',
        item['Worker_Type'] if item['Worker_Type'] else '',
        item['Effective_Date_of_Worker_Type'] if item['Effective_Date_of_Worker_Type'] else '',
        item['Exemption_Status'] if item['Exemption_Status'] else '',
        item['CF_LRV_Job_Exempt_Eff_Date'] if item['CF_LRV_Job_Exempt_Eff_Date'] else '',
        item['Gender'] if item['Gender'] else '',
        item['Hire_Date'] if item['Hire_Date'] else '',
        item['Termination_Date'] if item['Termination_Date'] else '',
        item['Active'] if item['Active'] else '',
        item['Function'] if item['Function'] else '',
        item['Function_Change_Effective_Date'] if item['Function_Change_Effective_Date'] else '',
        item['Business_Title'] if item['Business_Title'] else '',
        item['CF_LRV_Business_Title_Change'] if item['CF_LRV_Business_Title_Change'] else '',
        item['Field_HR'] if item['Field_HR'] else '',
        item['Manager_ID'] if item['Manager_ID'] else '',
        item['Effective_Date_of_Manager_Change'] if item['Effective_Date_of_Manager_Change'] else '',
        item['Work_Shift'] if item['Work_Shift'] else '',
        item['Work_Shift_Change_Effective_Date'] if item['Work_Shift_Change_Effective_Date'] else '',
        item['Location'] if item['Location'] else '',
        item['Location_Change_Eff_Date'] if item['Location_Change_Eff_Date'] else '',
        item['Country'] if item['Country'] else '',
        item['Date_of_Birth'] if item['Date_of_Birth'] else '',
        item['CF_LRV_Manager_Email'] if item['CF_LRV_Manager_Email'] else '',
        item['CF_LRV_Manager_First_Name'] if item['CF_LRV_Manager_First_Name'] else '',
        item['CF_LRV_Manager_Last_Name'] if item['CF_LRV_Manager_Last_Name'] else '',
        item['Legal_entity'] if item['Legal_entity'] else '',
        item['Worker_subType'] if item['Worker_subType'] else '',
        item['Cost_center'] if item['Cost_center'] else '',
        item['Worker_cc_change_date'] if item['Worker_cc_change_date'] else '',
        item['Year_of_service'] if item['Year_of_service'] else '',
        item['Paygroup'] if item['Paygroup'] else '',
        item['Japan_special_schedule_flag'] if item['Japan_special_schedule_flag'] else '',
        item['continous_service_date'] if item['continous_service_date'] else '',
        item['timeoff_service_date'] if item['timeoff_service_date'] else ''
    ]

def get_blank_fields_conf(dag_run, item):
    return [
        item['userid'],
        str(item['firstname']) + " " + str(item['lastname']),
        'validation',
        'Skipped',
        'User ID must be present',
        '',
        get_dagrun_ecid(dag_run)
    ]

def get_enabled_dept():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:effectively-enabled",
            "urn:replicon:department-group-list-column:full-path"            
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:department-group-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "bool":"true"
                }
            }
        }
    }

def process_each_user_payload(item):
    return {
        'userid': item['userid'],
        'workerreferenceemployeeid': item['workerreferenceemployeeid'],
        'emailaddress': item['emailaddress'],
        'firstname': item['firstname'],
        'lastname': item['lastname'],
        'workertype': item['workertype'],
        'effective_date_of_worker_type': item['effective_date_of_worker_type'],
        'exemptionstatus': item['exemptionstatus'],
        'cf_lrv_job_exempt_eff_date': item['cf_lrv_job_exempt_eff_date'],
        'gender': item['gender'],
        'hiredate': item['hiredate'],
        'terminationdate': item['terminationdate'],
        'active': item['active'],
        'function': item['function'],
        'function_change_effective_date': item['function_change_effective_date'],
        'businesstitle': item['businesstitle'],
        'cf_lrv_business_title_change': item['cf_lrv_business_title_change'],
        'fieldhr': item['fieldhr'],
        'managerid': item['managerid'],
        'effective_date_of_manager_change': item['effective_date_of_manager_change'],
        'work_shift': item['work_shift'],
        'work_shift_change_effective_date': item['work_shift_change_effective_date'],
        'location': item['location'],
        'location_change_eff_date': item['location_change_eff_date'],
        'country': item['country'],
        'date_of_birth': item['date_of_birth'],
        'cf_lrv_manager_email': item['cf_lrv_manager_email'],
        'cf_lrv_manager_first_name': item['cf_lrv_manager_first_name'],
        'cf_lrv_manager_last_name': item['cf_lrv_manager_last_name'],
        'legalentity': item['legalentity'],
        'worker_subType': item['worker_subType'],
        'cost_center': item['cost_center'],
        'worker_cc_change_date': item['worker_cc_change_date'],
        'year_of_service': item['year_of_service'],
        'paygroup': item['paygroup'],
        'japan_special_schedule_flag': item['japan_special_schedule_flag'],
        'continous_service_date': item['continous_service_date'],
        'timeoff_service_date': item['timeoff_service_date'],
        'logger' : rail.result('logger_list'),
        'supervisor_logger' : rail.result('supervisor_logger_list'),
        'departmentgroup' : rail.result('get_department_list'),
        'enabledcostcentre': rail.result('get_enabled_cost_centers'),
        'servc_centre': rail.result('get_enabled_service_centers'),
        'enableddivisions' : rail.result('get_all_enabled_divisions')
    }

def process_supervisor_mapper_data(item):
    return {
        "loginid": item['loginid'],
        "supervisorempid": item['supervisorempid'],
        "useruri": item['useruri'],
        'type': item['type'],
        "sup_email": item['sup_email'],
        "sup_firstname": item['sup_firstname'],
        "sup_lastname": item['sup_lastname'],
        "sup_change_effective_date": item['sup_change_effective_date'],
        'logger' : rail.result('logger_list'),
        'supervisor_logger' : rail.result('supervisor_logger_list'),
    }

def get_search_user_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:end-date",
            "urn:replicon:user-list-column:start-date",
            "urn:replicon:user-list-column:enabled"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['userid']
                }
            }
        }
    }

def process_disable_user_payload(dag_run):
    return {
        'userid': dag_run.conf['userid'],
        'workerreferenceemployeeid': dag_run.conf['workerreferenceemployeeid'],
        'emailaddress': dag_run.conf['emailaddress'],
        'firstname': dag_run.conf['firstname'],
        'lastname': dag_run.conf['lastname'],
        'workertype': dag_run.conf['workertype'],
        'effective_date_of_worker_type': dag_run.conf['effective_date_of_worker_type'],
        'exemptionstatus': dag_run.conf['exemptionstatus'],
        'cf_lrv_job_exempt_eff_date': dag_run.conf['cf_lrv_job_exempt_eff_date'],
        'gender': dag_run.conf['gender'],
        'hiredate': dag_run.conf['hiredate'],
        'terminationdate': rail.result('get_all_req_uri_details_40')['enddate'] if rail.result(
            'get_all_req_uri_details_40')['status'].lower() == 'false' else dag_run.conf['terminationdate'],
        'active': dag_run.conf['active'],
        'function': dag_run.conf['function'],
        'function_change_effective_date': dag_run.conf['function_change_effective_date'],
        'businesstitle': dag_run.conf['businesstitle'],
        'cf_lrv_business_title_change': dag_run.conf['cf_lrv_business_title_change'],
        'fieldhr': dag_run.conf['fieldhr'],
        'managerid': dag_run.conf['managerid'],
        'effective_date_of_manager_change': dag_run.conf['effective_date_of_manager_change'],
        'work_shift': dag_run.conf['work_shift'],
        'work_shift_change_effective_date': dag_run.conf['work_shift_change_effective_date'],
        'location': dag_run.conf['location'],
        'location_change_eff_date': dag_run.conf['location_change_eff_date'],
        'country': dag_run.conf['country'],
        'date_of_birth': dag_run.conf['date_of_birth'],
        'cf_lrv_manager_email': dag_run.conf['cf_lrv_manager_email'],
        'cf_lrv_manager_first_name': dag_run.conf['cf_lrv_manager_first_name'],
        'cf_lrv_manager_last_name': dag_run.conf['cf_lrv_manager_last_name'],
        'departmentgroupuri': rail.result('get_all_req_uri_details_40')['departmentgroupuri'],
        'useruri': rail.result('get_all_req_uri_details_40')['useruri'],
        'logger': rail.result('create_user_log'),
        'supervisor_logger': dag_run.conf['supervisor_logger']
    }

def process_update_user_payload(dag_run):
    return {
        'userid': dag_run.conf['userid'],
        'workerreferenceemployeeid': dag_run.conf['workerreferenceemployeeid'],
        'emailaddress': dag_run.conf['emailaddress'],
        'firstname': dag_run.conf['firstname'],
        'lastname': dag_run.conf['lastname'],
        'workertype': dag_run.conf['workertype'],
        'effective_date_of_worker_type': dag_run.conf['effective_date_of_worker_type'],
        'exemptionstatus': dag_run.conf['exemptionstatus'],
        'cf_lrv_job_exempt_eff_date': dag_run.conf['cf_lrv_job_exempt_eff_date'],
        'gender': dag_run.conf['gender'],
        'hiredate': dag_run.conf['hiredate'],
        'terminationdate': dag_run.conf['terminationdate'],
        'active': dag_run.conf['active'],
        'function': dag_run.conf['function'],
        'function_change_effective_date': dag_run.conf['function_change_effective_date'],
        'businesstitle': dag_run.conf['businesstitle'],
        'cf_lrv_business_title_change': dag_run.conf['cf_lrv_business_title_change'],
        'fieldhr': dag_run.conf['fieldhr'],
        'managerid': dag_run.conf['managerid'],
        'effective_date_of_manager_change': dag_run.conf['effective_date_of_manager_change'],
        'work_shift': dag_run.conf['work_shift'],
        'work_shift_change_effective_date': dag_run.conf['work_shift_change_effective_date'],
        'location': dag_run.conf['location'],
        'location_change_eff_date': dag_run.conf['location_change_eff_date'],
        'country': dag_run.conf['country'],
        'date_of_birth': dag_run.conf['date_of_birth'],
        'cf_lrv_manager_email': dag_run.conf['cf_lrv_manager_email'],
        'cf_lrv_manager_first_name': dag_run.conf['cf_lrv_manager_first_name'],
        'cf_lrv_manager_last_name': dag_run.conf['cf_lrv_manager_last_name'],
        'departmentgroupuri': rail.result('get_all_req_uri_details_40')['departmentgroupuri'],
        'useruri': rail.result('get_all_req_uri_details_40')['useruri'],
        'rehireupdate': 'rehire' if rail.result(
            'get_all_req_uri_details_40')['status'].lower() == 'false' else 'update',
        'legalentity': dag_run.conf['legalentity'],
        'worker_subType': dag_run.conf['worker_subType'],
        'cost_center': dag_run.conf['cost_center'],
        'worker_cc_change_date': dag_run.conf['worker_cc_change_date'],
        'year_of_service': dag_run.conf['year_of_service'],
        'paygroup': dag_run.conf['paygroup'],
        'continous_service_date': dag_run.conf['continous_service_date'],
        'timeoff_service_date': dag_run.conf['timeoff_service_date'],
        'legalentityuri': rail.result('get_all_req_uri_details_40')['legalentityuri'],
        'paygroupuri' :rail.result('get_all_req_uri_details_40')['paygroupuri'],
        'costcenteruri' :rail.result('get_all_req_uri_details_40')['costcenteruri'],
        'logger': rail.result('create_user_log'),
        'supervisor_logger': dag_run.conf['supervisor_logger']
    }

def process_add_user_payload(dag_run):
    return {
        'userid': dag_run.conf['userid'],
        'workerreferenceemployeeid': dag_run.conf['workerreferenceemployeeid'],
        'emailaddress': dag_run.conf['emailaddress'],
        'firstname': dag_run.conf['firstname'],
        'lastname': dag_run.conf['lastname'],
        'workertype': dag_run.conf['workertype'],
        'effective_date_of_worker_type': dag_run.conf['effective_date_of_worker_type'] if dag_run.conf[
            'effective_date_of_worker_type'] else str(datetime.now().date()),
        'exemptionstatus': dag_run.conf['exemptionstatus'],
        'cf_lrv_job_exempt_eff_date': dag_run.conf['cf_lrv_job_exempt_eff_date'],
        'gender': dag_run.conf['gender'],
        'hiredate': dag_run.conf['hiredate'],
        'terminationdate': dag_run.conf['terminationdate'],
        'active': dag_run.conf['active'],
        'function': dag_run.conf['function'],
        'function_change_effective_date': dag_run.conf['function_change_effective_date'],
        'businesstitle': dag_run.conf['businesstitle'],
        'cf_lrv_business_title_change': dag_run.conf['cf_lrv_business_title_change'] if dag_run.conf[
            'cf_lrv_business_title_change'] else str(datetime.now().date()),
        'fieldhr': dag_run.conf['fieldhr'],
        'managerid': dag_run.conf['managerid'],
        'effective_date_of_manager_change': dag_run.conf['effective_date_of_manager_change'] if dag_run.conf[
            'effective_date_of_manager_change'] else str(datetime.now().date()),
        'work_shift': dag_run.conf['work_shift'],
        'work_shift_change_effective_date': dag_run.conf['work_shift_change_effective_date'],
        'location': dag_run.conf['location'],
        'location_change_eff_date': dag_run.conf['location_change_eff_date'] if dag_run.conf[
            'location_change_eff_date'] else str(datetime.now().date()),
        'country': dag_run.conf['country'],
        'date_of_birth': dag_run.conf['date_of_birth'],
        'cf_lrv_manager_email': dag_run.conf['cf_lrv_manager_email'],
        'cf_lrv_manager_first_name': dag_run.conf['cf_lrv_manager_first_name'],
        'cf_lrv_manager_last_name': dag_run.conf['cf_lrv_manager_last_name'],
        'departmentgroupuri': rail.result('get_all_req_uri_details_40')['departmentgroupuri'],
        'legalentity': dag_run.conf['legalentity'],
        'worker_subType': dag_run.conf['worker_subType'],
        'cost_center': dag_run.conf['cost_center'],
        'worker_cc_change_date': dag_run.conf['worker_cc_change_date'],
        'year_of_service': dag_run.conf['year_of_service'],
        'paygroup': dag_run.conf['paygroup'],
        'continous_service_date': dag_run.conf['continous_service_date'],
        'timeoff_service_date': dag_run.conf['timeoff_service_date'],
        'legalentityuri': rail.result('get_all_req_uri_details_40')['legalentityuri'],
        'paygroupuri' :rail.result('get_all_req_uri_details_40')['paygroupuri'],
        'costcenteruri' :rail.result('get_all_req_uri_details_40')['costcenteruri'],
        'logger': rail.result('create_user_log'),
        'supervisor_logger': dag_run.conf['supervisor_logger']
    }

def user_import_log_csv_data(item):
    return [
        item['userid'],
        item['username'],
        item['action'],
        item['status'],
        item['details'],
        item['country'],
        item['childjobid']
    ]

def log_user_disable_payload(dag_run):
    if dag_run.conf['terminationdate']:
        details = "User profile disabled successfully with end date ;"
    else:
        details = "User profile disabled successfully however no end date was received ;"
    return {
        "userid": dag_run.conf['userid'],
        "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
        "action": "Disable user",
        "status": "Success",
        'details': details,
        'country':''
    }

def log_process_user_payload(dag_run):
    action = "Add"
    details = "User is  disabled in workday hence not added"
    if rail.result('get_all_req_uri_details_40')['useruri']:
        action = "Disable user"
        details = "User status (Active) received blank value or '-'"
        if str(rail.result('get_all_req_uri_details_40')['status']).lower() == 'false':
            if rail.result('get_all_req_uri_details_40')['enddate']:
                details = "User is already disabled in Replicon with end date"
            else:
                if (datetime.strptime(dag_run.conf['terminationdate'], "%Y-%m-%d")).date() < datetime.now().date():
                    details = "User not disabled since end date received is in the past"
                elif (datetime.strptime(dag_run.conf['terminationdate'], "%Y-%m-%d")).date() < datetime.strptime(
                    rail.result('get_all_req_uri_details_40')['startdate'], "%Y-%m-%d").date():
                    details = "User was already disabled in Replicon, end date was updated since end date received is in the past"
    return {
        "userid": dag_run.conf['userid'],
        "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
        "action": action,
        "status": "Skipped",
        'details': details,
        'country':''
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

def add_missing_supervisor_permission_payload():
    return {
        'userUri': rail.result('search_for_user_with_empid')[0]['uri'],
        'permissionSetUri': rail.result('get_all_permissionsets')['supervisor']
    }

def assign_policydataaccessscope_department(dag_run):
    return {
        "userUri": dag_run.conf['useruri'] if 'useruri' in dag_run.conf else rail.result('create_user')['uri'],
        "policyDataAccessScopes":[{
            "policyUri":"urn:replicon:policy:time-off",
            "departmentGroups":[{
                "departmentGroup":{
                    "uri":dag_run.conf['departmentgroupuri']
                }
            }]
        }]
    }

def supervisor_assignment_log_payload(dag_run):
    return {
        "loginid": dag_run.conf['userid'],
        "supervisorempid": dag_run.conf['managerid'],
        "useruri": dag_run.conf['useruri'] if 'useruri' in dag_run.conf else rail.result('create_user')['uri'],
        'type': "update" if 'useruri' in dag_run.conf else "add",
        "sup_email": dag_run.conf['CF_LRV_Manager_Email'] if dag_run.conf['CF_LRV_Manager_Email'] else '',
        "sup_firstname": dag_run.conf['CF_LRV_Manager_First_Name'] if dag_run.conf['CF_LRV_Manager_First_Name'] else '',
        "sup_lastname": dag_run.conf['CF_LRV_Manager_Last_Name'] if dag_run.conf['CF_LRV_Manager_Last_Name'] else '',
        "sup_change_effective_date": dag_run.conf['Effective_Date_of_Manager_Change'] \
            if dag_run.conf['Effective_Date_of_Manager_Change'] else str(datetime.strftime(datetime.now().date(), '%Y-%m-%d')),
    }

def search_supervisor_payload():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:login-name"
        ]
    }

def get_default_timeofftype_policy_sched_payload(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri'] if 'timeoffuri' in dag_run.conf else rail.result('foreach_timeoffuri')['uri']
        }
    }

def trigger_timeoff_add_rehire_payload(item, dag_run):
    return {
        "loginname": dag_run.conf['useruri'],
        "startdate": dag_run.conf['hiredate'],
        "terminationdate": dag_run.conf['terminationdate'],
        "active": dag_run.conf['active'],
        "useruri": dag_run.conf['useruri'],
        'tiemofftypes':item['name'],
        'continuousservicedate':dag_run.conf['continuousservicedate'],
        "rehire": dag_run.conf['rehire'],
        "timeoffuri": item['uri']
    }

def trigger_child_0_balance_timeoff_payload(item, dag_run):
    return {
        "userid": dag_run.conf['useruri'],
        "hiredate": dag_run.conf['hiredate'],
        "terminationdate": dag_run.conf['terminationdate'],
        "active": dag_run.conf['active'],
        "useruri": dag_run.conf['useruri'],
        "timeoffupdate": 'yes',
        "timeoffuri": item
    }

def get_timesheet_for_date2_payload(dag_run):
    todays_date = datetime.now()
    return {
        "userUri": dag_run.conf['useruri'],
        "date": {
            "day": todays_date.day,
            "month": todays_date.month,
            "year": todays_date.year
        },
        "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
    }

MANDATORY_FIELDS = {
    "userid":"userid"
}

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")

def get_invalid_record(item):
    details = get_mandatory_fields_exception_message(item)
    return {
        "userid": item['userid'],
        "username": item['firstname'] + " " + item['lastname'],
        "action": "validation",
        "status": "Skipped",
        'details': details,
        'country': ''
    }
