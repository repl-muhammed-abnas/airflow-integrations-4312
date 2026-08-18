from datetime import datetime
from rail import find_first_by_attr_and_get_attr, result, load_all_records, get_current_context
from rail.lib.ecid import get_dagrun_ecid
from impervainc.user_sync.utils.python_callable import get_originalhiredate

def add_usersync_payload(item):
    rit_dept_data = load_all_records(result('load_rit_dept_lookup_report_data'))
    name = "Imperva Inc / " + str(item["Cost_Center_Name"])
    return {
        "parentjobid": get_dagrun_ecid(get_current_context()['dag_run']),
        "status": item["status"],
        "Employee_ID": item["Employee_ID"],
        "Legal_First_Name": item["Legal_First_Name"],
        "Legal_Last_Name": item["Legal_Last_Name"],
        "primaryWorkEmail": item["primaryWorkEmail"],
        "Username": item["Username"],
        "Authentication_ID": item["Authentication_ID"],
        "Hire_Date": item["Hire_Date"],
        "Original_Hire_Date": item["Original_Hire_Date"],
        "termination_date": item["termination_date"],
        "Manager": item["Manager"],
        "Imperva_Worker_Type": item["Imperva_Worker_Type"],
        "Imperva_Employee_Type": item["Imperva_Employee_Type"],
        "Time_Type": item["Time_Type"],
        "Pay_Rate_Type": item["Pay_Rate_Type"],
        "Hourly_Pay": item["Hourly_Pay"],
        "Currency": item["Currency"],
        "Job_Code": item["Job_Code"],
        "Cost_Center_ID": item["Cost_Center_ID"],
        "Cost_Center_Name": item["Cost_Center_Name"],
        "Imperva_Organization": item["Imperva_Organization"],
        "timezone": item["timezone"],
        "Work_Address_Country": item["Work_Address_Country"],
        "Country_ISO_Code": item["Country_ISO_Code"],
        "Work_Address_State_Province": item["Work_Address_State_Province"],
        "State_ISO_Code": item["State_ISO_Code"],
        "Exempt_Status": item["Exempt_Status"],
        "isManager": item["isManager"],
        "departmenturi": find_first_by_attr_and_get_attr(rit_dept_data, 'Department Full Name', name, 'uri'),
        "costcenteruri": find_first_by_attr_and_get_attr(result('get_all_cost_center'),'displayText', item["Cost_Center_Name"],'uri'),
        "supervisor_sync_log": result('imperva_supervisor_sync_logs'),
        "user_sync_log": result('imperva_user_sync_logs')
    }

def update_usersync_payload(item):
    resp = add_usersync_payload(item)
    reference_data = load_all_records(result('load_user_reference_report_data'))
    resp['useruri'] = find_first_by_attr_and_get_attr(reference_data, 'Login Name', item["Username"], 'uri')
    return resp

def disable_user_payload(dag_run):
    return {
        "parentjobid": dag_run.conf['parentjobid'],
        "status": dag_run.conf['status'],
        "Employee_ID": dag_run.conf['Employee_ID'],
        "Legal_First_Name": dag_run.conf['Legal_First_Name'],
        "Legal_Last_Name": dag_run.conf['Legal_Last_Name'],
        "primaryWorkEmail": dag_run.conf['primaryWorkEmail'],
        "Username": dag_run.conf['Username'],
        "Authentication_ID": dag_run.conf['Authentication_ID'],
        "Hire_Date": dag_run.conf['Hire_Date'],
        "Original_Hire_Date": dag_run.conf['Original_Hire_Date'],
        "termination_date": dag_run.conf['termination_date'],
        "Manager": dag_run.conf['Manager'],
        "Imperva_Worker_Type": dag_run.conf['Imperva_Worker_Type'],
        "Imperva_Employee_Type": dag_run.conf['Imperva_Employee_Type'],
        "Time_Type": dag_run.conf['Time_Type'],
        "Pay_Rate_Type": dag_run.conf['Pay_Rate_Type'],
        "Hourly_Pay": dag_run.conf['Hourly_Pay'],
        "Currency": dag_run.conf['Currency'],
        "Job_Code": dag_run.conf['Job_Code'],
        "Cost_Center_ID": dag_run.conf['Cost_Center_ID'],
        "Cost_Center_Name": dag_run.conf['Cost_Center_Name'],
        "Imperva_Organization": dag_run.conf['Imperva_Organization'],
        "timezone": dag_run.conf['timezone'],
        "Work_Address_Country": dag_run.conf['Work_Address_Country'],
        "Country_ISO_Code": dag_run.conf['Country_ISO_Code'],
        "Work_Address_State_Province": dag_run.conf['Work_Address_State_Province'],
        "State_ISO_Code": dag_run.conf['State_ISO_Code'],
        "Exempt_Status": dag_run.conf['Exempt_Status'],
        "isManager": dag_run.conf['isManager'],
        "departmenturi": dag_run.conf['departmenturi'],
        "costcenteruri": dag_run.conf['costcenteruri'],
        "supervisor_sync_log": dag_run.conf['supervisor_sync_log'],
        "user_sync_log": dag_run.conf['user_sync_log'],
        "useruri": dag_run.conf.get('useruri', '')
    }

def get_timeoff_add_payload(item, dag_run):
    resp = disable_user_payload(dag_run)
    resp['useruri'] = result('create_user_in_replicon')['uri']
    resp['timeoffuri'] = item['uri']
    resp['timeofftypename'] = item['name']
    return resp

def timeoff_assignment_payload(dag_run):
    resp = disable_user_payload(dag_run)
    resp['rehire_update'] = "rehire" if result('user_rehired_18') else result('get_timeoff_trigger')['value']
    return resp

def get_timeoffnameswithuri_payload(item, dag_run):
    resp = disable_user_payload(dag_run)
    resp['rehire_update'] = dag_run.conf['rehire_update']
    resp['timeoffuri'] = item['uri']
    resp['timeofftypename'] = item['name']
    return resp

def update_user_payroll(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "hourlyRate": {
            "amount": dag_run.conf['Hourly_Pay'],
            "currencyUri": result('get_currency_uri')
        },
        "dateRange": {
            "startDate": get_originalhiredate(dag_run),
            "endDate": None,
            "relativeDateRangeUri": None,
            "relativeDateRangeAsOfDate": None
        }
    }

def get_search_user_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:user"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['Manager'] if dag_run.conf.get('Manager') else dag_run.conf['supervisorloginname']
                },
                "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        }
    }

def get_user_create_payload(dag_run):
    return {
        "user": {
            "target": {
                "uri": None,
                "loginName": dag_run.conf['Username'],
                "parameterCorrelationId": None
            },
            "firstname": dag_run.conf['Legal_First_Name'],
            "lastname": dag_run.conf['Legal_Last_Name'],
            "emailAddress": result('final_email_address'),
            "employeeId": result('final_employee_id'),
            "department": {
                "uri": dag_run.conf['departmenturi'],
                "name": None,
                "parent": None,
                "parameterCorrelationId": None
            },
            "supervisorAssignmentSchedule": None,
            "schedulePolicySchedule": [
                {
                    "schedulePolicy": {
                    "officeScheduleUri": None,
                    "name": result('search_schedule_value'),
                    "officeSchedule": None,
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": None
                }
            ],
            "workWeekStartDayUri": None,
            "employmentDateRange": {
            "startDate": get_originalhiredate(dag_run),
            "endDate": None,
            "relativeDateRangeUri": None,
            "relativeDateRangeAsOfDate": None
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['Username'],
                "SSOName": dag_run.conf['Username'],
                "password": None
            },
            "holidayCalendar": None,
            "timeOffPolicy": None,
            "permissionSets": result('create_permissionset_list'),
            "policySets": [],
            "employeeType": {
                "uri": result('get_required_employee_type_uri'),
                "name": None
            },
            "timesheetPeriodTypeUri": result('get_timesheetperiod_28')['value'],
            "costRateSchedule": None,
            "payrollRateSchedule": None,
            "defaultBillingRate": None,
            "timesheetApprovalPath": {
                "uri": None,
                "name": result('search_timesheet_approval_path_value_50')
            },
            "expenseApprovalPath": None,
            "timeOffApprovalPath": {
                "uri": None,
                "name": result('search_timeoff_approval_path_value_51')
            },
            "customFieldValues": [],
            "assignedActivities": [],
            "timeZone": {
                "uri": result('get_timezone_uri_to_assign'),
                "IANAName": None
            },
            "overtimeRuleAssignmentSchedule": None,
            "validationRuleAssignmentSchedule": None,
            "locationSchedule": [],
            "divisionSchedule": [],
            "costCenterSchedule": [
                {
                    "costCenter": {
                        "uri": dag_run.conf['costcenteruri'],
                        "parentUri": None,
                        "name": None
                    },
                    "effectiveDate": None
                }
            ],
            "serviceCenterSchedule": [],
            "departmentGroupSchedule": [],
            "employeeTypeGroupSchedule": [],
            "timesheetPeriodSchedule": [],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": [
                {
                    "payRuleScript": {
                        "uri": None,
                        "name": "*Imperva - Payrule Placeholder"
                    },
                    "effectiveDate": None
                }
            ]
        }
    }

def get_put_timeoff_account_policyset_schedule_payload(dag_run, schedule_for_timeoff):
    effective_date = datetime.strptime(dag_run.conf['terminationdate'], '%m/%d/%Y').date()
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri']
        },
        "policySetScheduleEntries": schedule_for_timeoff +
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
                                    "number": dag_run.conf['balance'],
                                    "collection": []
                                }
                            },
                            {
                                "keyUri": "urn:replicon:script-key:parameter:precedence",
                                "value": {
                                    "number": 20,
                                    "collection": []
                                }
                            }
                        ]
                    }
                ],
                "timeOffValidationScripts": [{
                    "scriptTarget":{"uri":dag_run.conf['preventbalanceoverdrawuri']},
                    "additionalParameters":[
                        {"keyUri":"urn:replicon:script-key:parameter:maximum-overdraw","value":{"number":"0"}}
                    ]
                }]
            }
        }]
    }

def get_search_supervisor_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:user"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['supervisorloginname']
                },
                "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        }
    }

def get_supervisor_assignment_payload(item):
    return {
        "loginname": item['properties']['loginname'],
        "supervisorloginname": item['properties']['supervisorid'],
        "useruri": item['properties']['enduseruri'],
        "parentjobid": item['properties']['parentjobid'],
        "type": item['properties']['status'],
        "supervisor_sync_log": result('imperva_supervisor_sync_logs'),
        "user_sync_log": result('imperva_user_sync_logs')
    }
