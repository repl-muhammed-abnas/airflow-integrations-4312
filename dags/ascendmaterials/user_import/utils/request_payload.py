from hashlib import md5
import rail

null=None

# pylint: disable=unused-variable too-many-branches too-many-statements line-too-long
def user_import_csv_data(item):
    return [
            item['loginname'].strip() if item['loginname'] else '',
            item['employeefirstname'].strip() if item['employeefirstname'] else '',
            item['employeelastname'].strip() if item['employeelastname'] else '',
            item['employeetype'].strip() if item['employeetype'] else '',
            item['timetype'].strip() if item['timetype'] else '',
            item['department'].strip() if item['department'] else '',
            item['authenticationtype'].strip() if item['authenticationtype'] else '',
            item['enabled'].strip() if item['enabled'] else '',
            item['employeeid'].strip() if item['employeeid'] else '',
            item['startdate'],
            item['lastdayofwork'],
            item['continuousservicedate'],
            item['emailaddress'].strip() if item['emailaddress'] else '',
            item['manager'].strip() if item['manager'] else '',
            item['location'].strip() if item['location'] else '',
            item['homecountry'].strip() if item['homecountry'] else '',
            item['homestateprovince'].strip() if item['homestateprovince'] else '',
            item['homecity'].strip() if item['homecity'] else '',
            item['hourlypayrollrate'].strip() if item['hourlypayrollrate'] else '',
            item['hourlypayrollcurrency'].strip() if item['hourlypayrollcurrency'] else '',
            item['costcenter'].strip() if item['costcenter'] else '',
            item['udf'].strip() if item['udf'] else '',
            md5((str(str(item['loginname']) + str(item['employeefirstname']) + str(item['employeelastname']) + str(item['employeetype']) +
                                str(item['timetype']) + str(item['department']) + str(item['authenticationtype']) + str(item['enabled']) +
                                str(item['employeeid']) + str(item['startdate']) + str(item['lastdayofwork']) +
                                str(item['continuousservicedate']) + str(item['emailaddress']) +
                                str(item['manager']) + str(item['location']) + str(item['homecountry']) + str(item['homestateprovince']) +
                                str(item['homecity']) + str(item['hourlypayrollrate']) + str(item['hourlypayrollcurrency']) + str(item['costcenter']) +
                                str(item['udf'])
            )).encode('utf-8')).hexdigest()
        ]


def get_search_payload_data():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-id"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": rail.result('foreach_query_list_new_changed_profiles')['employeeid']
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_filtered_user_data(response):
    return response.json()['d']['rows']


def get_search_user_3_payload_data(dag_run):
    return {
        'page': '1',
        'pagesize': '100',
        'columnUris': [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:enabled"
        ],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name'
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {
                    'text': dag_run.conf['supervisorloginname']
                }
            }
        }
    }


def create_user_24_paload_data(dag_run):
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['loginname'],
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf['employeefirstname'],
            "lastname": dag_run.conf['employeelastname'],
            "emailAddress": dag_run.conf['emailaddress'],
            "employeeId": dag_run.conf['employeeid'],
            "department": {
                "uri": null,
                "name": rail.result('get_company_department')['name'],
                "parent": null,
                "parameterCorrelationId": null
            },
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [],
            "workWeekStartDayUri": null,
            "employmentDateRange": {
                "startDate": {
                    "year": rail.result('get_start_date')['year'],
                    "month": rail.result('get_start_date')['month'],
                    "day": rail.result('get_start_date')['day']
                },
                "endDate": null,
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
            "holidayCalendar": null,
            "timeOffPolicy": null,
            "permissionSets": [
                    {
                        "uri": null,
                        "name": "Basic User"
                    }
                ],
            "policySets": [],
            "employeeType": {
                "uri": rail.result('get_all_employee_type_details'),
                "name": null
            },
            "timesheetPeriodTypeUri": "urn:replicon:timesheet-period-type:system",
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": null,
            "expenseApprovalPath": null,
            "timeOffApprovalPath": null,
            "customFieldValues": [ ],
            "assignedActivities": [],
            "timeZone": null,
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": [],
            "divisionSchedule": [],
            "costCenterSchedule": [],
            "serviceCenterSchedule": [],
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": [],
            "payRuleScriptSchedule": []
        }
    }

def get_data_for_costcenter_payload_data():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
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
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:cost-center-list-filter:text"
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
                "text": rail.result('log_requiredcostcentername'),
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
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_data_for_required_user_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:user-list-column:supervisor",
            "urn:replicon:user-list-column:location",
            "urn:replicon:user-list-column:cost-center"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon:user-list-filter:user"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": {
                "uri": dag_run.conf['useruri'],
                "uris": [],
                "bool": null,
                "date": null,
                "money": null,
                "number": null,
                "text": null,
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


def get_cost_center_payload(dag_run):
    cost_center = dag_run.conf['costcenter'].split(" | ")[-1].strip() if dag_run.conf['costcenter'] else ''
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
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
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:cost-center-list-filter:text"
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
                "text": cost_center,
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
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }
