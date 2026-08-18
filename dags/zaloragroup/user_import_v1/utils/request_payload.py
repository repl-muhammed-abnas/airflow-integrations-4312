from datetime import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid

def get_today_dateformat_payload():
    return {
        "year": datetime.now().strftime("%Y"),
        "month": datetime.now().strftime("%m"),
        "day": datetime.now().strftime("%d")
    }

def get_date_format_paylod(date):
    return {
        "year": date.split('/')[2],
        "month": date.split('/')[1],
        "day": date.split('/')[0]
    }

def process_user_data(item):
    return {
        "parentjobid": get_dagrun_ecid(rail.get_current_context()['dag_run']),
        "companykey": rail.get_company_key(),
        "loginname": item['LOGIN_NAME'],
        "firstname": item['FIRST_NAME'],
        "lastname": item['LAST_NAME'],
        "employeetype": item['EMPLOYEE_TYPE'],
        "department": item['DEPARTMENT'].replace("-", "/"),
        "enabled": item['ENABLED'],
        "employeeid": item['EMPLOYEE_ID'],
        "startdate": item['START_DATE'].replace("-", "/"),
        "enddate": item['END_DATE'].replace("-", "/"),
        "emailaddress": item['EMAIL_ADDRESS'],
        "initialsupervisorloginname": item['INITIAL_SUPERVISOR_LOGINNAME'],
        "holidaycalendar": item['HOLIDAY_CALENDAR'],
        "subdepartment": item['SUB_DEPARTMENT'],
        "jobfamily": item['JOB_FAMILY'],
        "jobname": item['JOB_NAME'],
        "gradename": item['GRADE_NAME'],
        "legalentity": item['LEGAL_ENTITY'],
        "logger" : rail.result('logger_list'),
        "supervisor_mapper" : rail.result('supervisor_mapper_list')
    }

def process_add_user_data(dag_run):
    return {
        "parentjobid": get_dagrun_ecid(rail.get_current_context()['dag_run']),
        "companykey": rail.get_company_key(),
        "loginname": dag_run.conf['loginname'],
        "firstname": dag_run.conf['firstname'],
        "lastname": dag_run.conf['lastname'],
        "employeetype": dag_run.conf['employeetype'],
        "department": dag_run.conf['department'],
        "enabled": dag_run.conf['enabled'],
        "employeeid": dag_run.conf['employeeid'],
        "startdate": dag_run.conf['startdate'],
        "enddate": dag_run.conf['enddate'],
        "emailaddress": dag_run.conf['emailaddress'],
        "initialsupervisorloginname": dag_run.conf['initialsupervisorloginname'],
        "holidaycalendar": dag_run.conf['holidaycalendar'],
        "subdepartment": dag_run.conf['subdepartment'],
        "jobfamily": dag_run.conf['jobfamily'],
        "jobname": dag_run.conf['jobname'],
        "gradename": dag_run.conf['gradename'],
        "legalentity": dag_run.conf['legalentity'],
        "logger" : dag_run.conf['logger'],
        "supervisor_mapper" : dag_run.conf['supervisor_mapper']
    }

def process_update_user_data(dag_run):
    return {
        "parentjobid": get_dagrun_ecid(rail.get_current_context()['dag_run']),
        "companykey": rail.get_company_key(),
        "loginname": dag_run.conf['loginname'],
        "firstname": dag_run.conf['firstname'],
        "lastname": dag_run.conf['lastname'],
        "employeetype": dag_run.conf['employeetype'],
        "department": dag_run.conf['department'],
        "enabled": dag_run.conf['enabled'],
        "employeeid": dag_run.conf['employeeid'],
        "startdate": dag_run.conf['startdate'],
        "enddate": dag_run.conf['enddate'],
        "emailaddress": dag_run.conf['emailaddress'],
        "initialsupervisorloginname": dag_run.conf['initialsupervisorloginname'],
        "holidaycalendar": dag_run.conf['holidaycalendar'],
        "subdepartment": dag_run.conf['subdepartment'],
        "jobfamily": dag_run.conf['jobfamily'],
        "jobname": dag_run.conf['jobname'],
        "gradename": dag_run.conf['gradename'],
        "legalentity": dag_run.conf['legalentity'],
        "useruri": rail.result('check_user_present'),
        "logger" : dag_run.conf['logger'],
        "supervisor_mapper" : dag_run.conf['supervisor_mapper']
    }

def get_userdetails(dag_run):
    return {
            "page": "1",
            "pagesize": "100",
            "columnUris": [
                "urn:replicon:user-list-column:login-name"
            ],
            "filterExpression": {
                "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                "value": {
                    "text": dag_run.conf['loginname']
                }
                }
            }
        }

def get_supervisordetails(dag_run):
    return {
            "page": "1",
            "pagesize": "100",
            "columnUris": [
                "urn:replicon:user-list-column:login-name"
            ],
            "filterExpression": {
                "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                "value": {
                    "text": dag_run.conf['initialsupervisorloginname']
                }
                }
            }
        }

def update_supervisor(dag_run):
    if 'useruri' in dag_run.conf:
        useruri = dag_run.conf['useruri']
    else:
        useruri = rail.result('create_user')['uri']
    return {
            "userUri": useruri,
            "supervisorUri": rail.result('check_if_supervisor_available'),
            "dateRange": {
                "startDate": get_today_dateformat_payload()
            }
        }

def update_supervisor_from_mapper():
    return {
            "userUri": rail.result('get_user_details'),
            "supervisorUri": rail.result('check_if_supervisor_available'),
            "dateRange": {
                "startDate": get_today_dateformat_payload()
            }
        }

def update_emp_start_date(dag_run):
    if 'useruri' in dag_run.conf:
        useruri = dag_run.conf['useruri']
    else:
        useruri = rail.result('create_user')['uri']
    return {
            "userUri": useruri,
            "dateRange": {
                "startDate": get_date_format_paylod(dag_run.conf['startdate'])
            }
        }

def update_emp_date(dag_run):
    if 'useruri' in dag_run.conf:
        useruri = dag_run.conf['useruri']
    else:
        useruri = rail.result('create_user')['uri']
    return {
            "userUri": useruri,
            "dateRange": {
                "startDate": get_date_format_paylod(dag_run.conf['startdate']),
                "endDate": get_date_format_paylod(dag_run.conf['enddate'])
            }
        }

def update_emp_end_date(dag_run):
    return {
            "userUri": dag_run.conf['useruri'],
            "dateRange": {
                "startDate": {
                    "year": rail.result('get_user_details')['employmentDateRange']['startDate']['year'],
                    "month": rail.result('get_user_details')['employmentDateRange']['startDate']['month'],
                    "day": rail.result('get_user_details')['employmentDateRange']['startDate']['day']
                },
                "endDate": get_date_format_paylod(dag_run.conf['enddate'])
            }
        }


def create_user_payload(dag_run):
    return {
            "user": {
                "target": {
                "loginName": dag_run.conf['loginname']
                },
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                "emailAddress": dag_run.conf['emailaddress'],
                "employeeId": dag_run.conf['employeeid'],
                "department": {
                "name": dag_run.conf['department']
                },
                "employmentDateRange": {
                "startDate": get_today_dateformat_payload()
                },
                "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['loginname']
                },
                "employeeType": {
                "name": dag_run.conf['employeetype']
                }
            }
            }

def schedule_policyschedule():
    return {
            "userUri": rail.result('create_user')['uri'],
            "scheduleEntries": [
                {
                "schedulePolicy": {
                    "name": "8 hours/day; Mon-Fri"
                },
                "effectiveDate": get_today_dateformat_payload()
                }
            ]
            }

def process_supervisor_mapper_data(item):
    return {
        "loginname": item['loginname'],
        "initialsupervisorloginname": item['supervisorid'],
        "logger" : rail.result('logger_list')
    }
