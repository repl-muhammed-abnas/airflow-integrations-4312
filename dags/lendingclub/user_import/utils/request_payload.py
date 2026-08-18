# pylint: disable=unused-variable too-many-branches too-many-statements
from datetime import datetime
import rail

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

def user_import_csv_data(item):
    return [
        item['Login ID'],
        item['Employee ID'],
        item['Name'],
        item['Last Name'],
        item['Hire Date'],
        item['Employee Type Code'],
        item['Employee Type'],
        item['Vendor'],
        item['Employee Status'],
        item['Department Code'],
        item['Department'],
        item['Manager ID'],
        item['Email'],
        item['Location Code'],
        item['Location'],
        item['Permission'],
        item['JobLevel'],
        item['ScrumTeam']
    ]

def get_divisiondata_on_code_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:division-list-column:code",
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:effectively-enabled"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:division-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['departmentcode']
                }
            }
        }
    }

def get_divisiondata_on_name_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:division-list-column:code",
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:effectively-enabled"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:division-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['departmentname']
                }
            }
        }
    }

def get_locationdata_on_code_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:location-list-column:code",
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:effectively-enabled"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:location-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['locationcode']
                }
            }
        }
    }

def get_locationdata_on_name_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:location-list-column:code",
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:effectively-enabled"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:location-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['locationname']
                }
            }
        }
    }

def get_customfield_dropdown_option_uris():
    existing_dropdowns_list = rail.result('get_all_custom_field_dropdown_option')
    final_dropdown_list = list(map(lambda x: {
        'target': {
            'uri': x['uri'],
            'name': x['displayText']
        },
        'name': x['displayText'],
        'isEnabled': x['isEnabled']
    }, existing_dropdowns_list)) if existing_dropdowns_list else []

    new_values_to_set = rail.load_all_records(
        rail.result('new_vendor_values'))

    final_dropdown_list.extend(map(lambda x: {
        'name': x['vendor'],
        'isEnabled': True
    }, new_values_to_set))

    return final_dropdown_list

def user_process_conf(item):
    return {
        "loginname" : item["loginname"],
        "empid" :item["empid"],
        "firstname" : item["firstname"],
        "lastname" : item["lastname"],
        "hiredate" : item["hiredate"],
        "employeetypecode" : item["employeetypecode"],
        "employeetype" : item["employeetype"],
        "vendor" : item["vendor"],
        "employeestatus" : item["employeestatus"],
        "departmentcode" : item["departmentcode"],
        "department" : item["department"],
        "managerid" : item["managerid"],
        "email" : item["email"],
        "locationcode" : item["locationcode"],
        "location" : item["location"],
        "permission": item["permission"],
        "joblevel" : item["joblevel"],
        "Scrum" : item["Scrum"],
        "logger" : rail.result('user_import_log'),
        "supervisor_logger" : rail.result('supervisor_assignment_log'),
        "vendor_uri": rail.result('get_all_custom_fields_for_required_group')['vendors_uri'],
        "joblevel_uri": rail.result('get_all_custom_fields_for_required_group')['joblevel_uri']
    }

def update_emp_start_date():
    data = rail.result('get_user_details')
    return {
            "userUri": data['user_uri'],
            "dateRange": {
                "startDate": {
                    "year": data['user_start_date']['year'],
                    "month": data['user_start_date']['month'],
                    "day": data['user_start_date']['day']
                }
            }
        }

def process_user_update(dag_run):
    return {
        "loginname" : dag_run.conf["loginname"],
        "empid" :dag_run.conf["empid"],
        "firstname" : dag_run.conf["firstname"],
        "lastname" : dag_run.conf["lastname"],
        "hiredate" : dag_run.conf["hiredate"],
        "employeetypecode" : dag_run.conf["employeetypecode"],
        "employeetypename" : dag_run.conf["employeetype"],
        "vendor" : dag_run.conf["vendor"],
        "employeestatus" : dag_run.conf["employeestatus"],
        "departmentcode" : dag_run.conf["departmentcode"],
        "department" : dag_run.conf["department"],
        "managerid" : dag_run.conf["managerid"],
        "email" : dag_run.conf["email"],
        "locationcode" : dag_run.conf["locationcode"],
        "location" : dag_run.conf["location"],
        "permission": dag_run.conf["permission"],
        "useruri": rail.result('get_user_details')['user_uri'],
        "action": "rehire" if rail.result('get_user_details')['user_status'] == "False" else "update",
        "joblevel" : dag_run.conf["joblevel"],
        "Scrum" : dag_run.conf["Scrum"],
        "logger" : dag_run.conf["logger"],
        "supervisor_logger" : dag_run.conf["supervisor_logger"],
        "vendor_uri" : dag_run.conf["vendor_uri"],
        "joblevel_uri" : dag_run.conf["joblevel_uri"]
    }

def process_user_add(dag_run):
    return {
        "loginname" : dag_run.conf["loginname"],
        "empid" :dag_run.conf["empid"],
        "firstname" : dag_run.conf["firstname"],
        "lastname" : dag_run.conf["lastname"],
        "hiredate" : dag_run.conf["hiredate"],
        "employeetypecode" : dag_run.conf["employeetypecode"],
        "employeetypename" : dag_run.conf["employeetype"],
        "vendor" : dag_run.conf["vendor"],
        "employeestatus" : dag_run.conf["employeestatus"],
        "departmentcode" : dag_run.conf["departmentcode"],
        "department" : dag_run.conf["department"],
        "managerid" : dag_run.conf["managerid"],
        "email" : dag_run.conf["email"],
        "locationcode" : dag_run.conf["locationcode"],
        "location" : dag_run.conf["location"],
        "permission": dag_run.conf["permission"],
        "joblevel" : dag_run.conf["joblevel"],
        "Scrum" : dag_run.conf["Scrum"],
        "logger" : dag_run.conf["logger"],
        "supervisor_logger" : dag_run.conf["supervisor_logger"],
        "vendor_uri" : dag_run.conf["vendor_uri"],
        "joblevel_uri" : dag_run.conf["joblevel_uri"]
    }

def update_emp_daterange(dag_run):
    return {
            "userUri": dag_run.conf['useruri'],
            "dateRange": {
                "startDate": {
                    "year": dag_run.conf['user_start_date']['year'],
                    "month": dag_run.conf['user_start_date']['month'],
                    "day": dag_run.conf['user_start_date']['day']
                },
                "endDate": get_today_dateformat_payload()
            }
        }

def add_user_payload(dag_run):

    empstatus = 0
    if dag_run.conf['employeestatus'].lower() == 'active':
        empstatus = 1

    policyset = ''
    if dag_run.conf['employeetypename'].lower() == 'contractors' or \
        dag_run.conf['employeetypename'].lower() == 'part time employee':
        policyset ="Part time/Contractor employees"
    if dag_run.conf['employeetypename'].lower() == 'full time employee':
        policyset ="Full time employees"

    permissionset = [{"name":"Project Resource"}]
    if dag_run.conf['permission'].lower() == 'supervisor':
        permissionset = [{"name":"Supervisor"}, {"name":"Project Resource with Reports"}]
    if dag_run.conf['permission'].lower() == 'management':
        permissionset = [{"name":"Supervisor"}, {"name":"Substitute user"}]
    if dag_run.conf['permission'].lower() == 'project manager view access':
        permissionset = [{"name":"Project Manager View Access"}, {"name":"Substitute user"}]

    return {
        "user": {
            "target": {
                "loginName": dag_run.conf['loginname']
            },
            "firstname": dag_run.conf['firstname'],
            "lastname": dag_run.conf['lastname'],
            "emailAddress": dag_run.conf['email'],
            "employeeId": dag_run.conf['empid'],
            "department": {
                "uri": rail.result('get_primary_department')['all_scrum_team']
            },
            "employmentDateRange": {
                "startDate": {
                    "year": dag_run.conf['hiredate'].split('-')[0],
                    "month": dag_run.conf['hiredate'].split('-')[1],
                    "day": dag_run.conf['hiredate'].split('-')[2]
                }
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": empstatus,
                "loginName": dag_run.conf['loginname'],
                "SSOName": dag_run.conf['loginname']
            },
            "permissionSets": permissionset,
            "policySets": [
                {
                    "name": policyset
                },
                {
                    "name": "Time Off"
                }
            ],
            "employeeType": {
                "uri": rail.result('get_employee_type_details')
            },
            "timesheetPeriodTypeUri": "urn:replicon:timesheet-period-type:system",
            "timesheetApprovalPath": {
                "name": "Supervisor"
            }
        }
    }

def search_location_filter(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:location-list-column:code",
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:effectively-enabled"
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:location-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf['locationcode']
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:or",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:location-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf['location']
                    }
                }
            }
        }
    }

def search_department_filter(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:division-list-column:code",
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:effectively-enabled"
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:division-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf['departmentcode']
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:or",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:division-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf['department']
                    }
                }
            }
        }
    }

def process_supervisor_mapper_data(item):
    return {
        "loginid": item['loginid'],
        "managerid": item['managerid'],
        "empid": item['empid'],
        "useruri": item['useruri'],
        "type": item['type'],
        "logger" : rail.result('user_import_log')

    }

def update_user_emp_daterange(dag_run):
    return {
            "userUri": dag_run.conf['useruri'],
            "dateRange": {
                "startDate": {
                    "year": rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']['year'],
                    "month": rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']['month'],
                    "day": rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']['day']
                }
            }
        }

def update_emp_daterange_hiredate(dag_run):
    return {
            "userUri": dag_run.conf['useruri'],
            "dateRange": {
                "startDate": {
                    "year": dag_run.conf['hiredate'].split('-')[0],
                    "month": dag_run.conf['hiredate'].split('-')[1],
                    "day": dag_run.conf['hiredate'].split('-')[2]
                }
            }
        }

def get_user_details(dag_run):
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:user-list-column:supervisor",
            "urn:replicon:user-list-column:location",
            "urn:replicon:user-list-column:division"
        ],
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
                    "uri": rail.result('get_supervisor_data')['assigned_supervisor_uri']
                }
            }
        }
    }

def update_supervisorassignment_overdaterange(dag_run):
    return {
        "userUri": dag_run.conf['useruri'],
        "supervisorUri": rail.result('search_for_user_with_empid')[0]['uri'],
        "dateRange": {
            "startDate" : get_today_dateformat_payload()
        }
    }
