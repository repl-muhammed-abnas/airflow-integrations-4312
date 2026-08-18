from datetime import datetime
import hashlib
import json
import uuid
from airflow.models import Variable
import pendulum
import rail

null = None

def get_formated_user_row(item):
    user_md5 = hashlib.md5((
        (item['employee_id'] or '') +
        (item['employee_first_name'] or '') +
        (item['employee_last_name'] or '') +
        (item['email'] or '') +
        (item['start_date'] or '') +
        (item['end_date'] or '') +
        (item['manager'] or '') +
        (item['work_location'] or '')).encode()).hexdigest()

    return {
        'employee_id': item['employee_id'],
        'employee_first_name': item['employee_first_name'],
        'employee_last_name': item['employee_last_name'],
        'email': item['email'],
        'start_date': item['start_date'],
        'end_date': item['end_date'],
        'manager': item['manager'],
        'work_location': item['work_location'],
        "md5": user_md5
    }.values()

def get_user_data(item_list):
    formatted_data = []
    for item in item_list:
        formatted_data.append({
            'employee_id': item.get('employeeNumber', ''),
            'employee_first_name': item.get('firstName', ''),
            'employee_last_name': item.get('lastName', ''),
            'email': item.get('email', {}).get('emailAddress', '') if item.get('email') else '',
            'start_date': item.get('employmentDateData', {}).get('hireDate', '') if item.get('employmentDateData') else '',
            'end_date': item.get('employmentDateData', {}).get('terminationDate', '') if item.get('employmentDateData') else '',
            'manager': item.get('positionData', {}).get('manager', {}).get('employeeNumber', '') if item.get('positionData') and item['positionData'].get('manager') else '',
            'work_location': item.get('workLocation', {}).get('name', '') if item.get('workLocation') else ''
        })
    return formatted_data

def get_all_user_details():
    return {
        "users": [
            {
                "employeeId": item['employee_id']
            } for item in rail.load_all_records(rail.result('query_delta_records'))
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_all_manager_user_details():
    return {
        "users": [
            {
                "employeeId": item['manager']
            } for item in rail.load_all_records(rail.result('query_delta_records')) if item['manager']
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def mandatory_fields_check(dag_run):
    return (dag_run.conf['employee_id'] and dag_run.conf['employee_first_name'] and dag_run.conf['employee_last_name']
            and dag_run.conf['email'])

def get_create_user_payload():
    user_data = rail.load_all_records(rail.result('query_user_records'))[0]
    start_date = datetime.strptime(
        user_data['start_date'].split('T')[0].strip(), '%Y-%m-%d').date()
    end_date = datetime.strptime(
        user_data['end_date'].split('T')[0].strip(), '%Y-%m-%d').date() if user_data['end_date'] else None
    return {
        "modifications": {
            "firstName": {
                "value": user_data['employee_first_name']
            },
            "lastName": {
                "value": user_data['employee_last_name']
            },
            "loginName": {
                "value": user_data['email']
            },
            "emailAddress": {
                "value": user_data['email']
            },
            "employeeId": {
                "value": user_data['employee_id']
            },
            "employmentDateRange": {
                "value": {
                    "startDate": {
                        "year": start_date.year,
                        "month": start_date.month,
                        "day": start_date.day
                    },
                    "endDate": {
                        "year": end_date.year,
                        "month": end_date.month,
                        "day": end_date.day
                    } if end_date else null
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": "false" if end_date else "true"
                    },
                    "ssoName": {
                        "value": user_data['email']
                    },
                    "ssoNameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name"
                }
            },
            "timesheetApprovalPath": {
                "value": {
                    "name": "Supervisor"
                }
            },
            "workWeekStartDay": {
                "value": {
                    "uri": "urn:replicon:day-of-week:monday"
                }
            },
            "timesheetTemplate": {
                "value": {
                    "name": "Standard Timesheet"
                }
            },
            "products": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "name": "Workforce Management"
                        }
                    ]
                }
            ],
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "name": "Supervisor"
                            }
                        },
                        {
                            "permissionSetPolicy": {
                                "name": "Project Resource"
                            }
                        }
                    ]
                }
            ],
            "locationSchedule": [
                {
                    "item": {
                        "name": user_data['work_location']
                    }
                }
            ],
            "timesheetPeriodSchedule": [
                {
                    "item": {
                        "name": "Weekly Starting on Monday"
                    }
                }
            ],
            "scheduleTypeSchedule": [
                {
                    "item": {
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                        "officeSchedule": {
                            "name": "8 hours/day; Mon-Fri"
                        }
                    }
                }
            ]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_search_user_payload(dag_run):
    return {
        "users": [
            {
                "employeeId": dag_run.conf['manager']
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_user_payload(dag_run):
    return {
        "users": [
            {
                "employeeId": dag_run.conf['employee_id']
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def assign_supervisor_permission_payload(dag_run):
    return {
        "target": {
            "employeeId": dag_run.conf['manager']
        },
        "modifications": {
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "name": "Supervisor"
                            }
                        }
                    ]
                }
            ]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def create_new_user_payload(dag_run):
    start_date = datetime.strptime(
        dag_run.conf['start_date'].split('T')[0].strip(), '%Y-%m-%d').date()
    end_date = datetime.strptime(
        dag_run.conf['end_date'].split('T')[0].strip(), '%Y-%m-%d').date() if dag_run.conf['end_date'] else None
    return {
        "modifications": {
            "firstName": {
                "value": dag_run.conf['employee_first_name']
            },
            "lastName": {
                "value": dag_run.conf['employee_last_name']
            },
            "loginName": {
                "value": dag_run.conf['email']
            },
            "emailAddress": {
                "value": dag_run.conf['email']
            },
            "employeeId": {
                "value": dag_run.conf['employee_id']
            },
            "employmentDateRange": {
                "value": {
                    "startDate": {
                        "year": start_date.year,
                        "month": start_date.month,
                        "day": start_date.day
                    },
                    "endDate": {
                        "year": end_date.year,
                        "month": end_date.month,
                        "day": end_date.day
                    } if end_date else null
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": "false" if end_date else "true"
                    },
                    "ssoName": {
                        "value": dag_run.conf['email']
                    },
                    "ssoNameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name"
                }
            },
            "timesheetApprovalPath": {
                "value": {
                    "name": "Supervisor"
                }
            },
            "workWeekStartDay": {
                "value": {
                    "uri": "urn:replicon:day-of-week:monday"
                }
            },
            "timesheetTemplate": {
                "value": {
                    "name": "Standard Timesheet"
                }
            },
            "products": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "name": "Workforce Management"
                        }
                    ]
                }
            ],
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "name": "Project Resource"
                            }
                        }
                    ]
                }
            ],
            "locationSchedule": [
                {
                    "item": {
                        "name": dag_run.conf['work_location']
                    }
                }
            ],
            "supervisorSchedule": [
                {
                    "item": {
                        "employeeId": dag_run.conf['manager']
                    }
                }
            ] if dag_run.conf['manager'] and bool(rail.result('search_manager_user')) else [],
            "timesheetPeriodSchedule": [
                {
                    "item": {
                        "name": "Weekly Starting on Monday"
                    }
                }
            ],
            "scheduleTypeSchedule": [
                {
                    "item": {
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                        "officeSchedule": {
                            "name": "8 hours/day; Mon-Fri"
                        }
                    }
                }
            ]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def update_user_payload(dag_run,config):
    start_date = datetime.strptime(
        dag_run.conf['start_date'].split('T')[0].strip(), '%Y-%m-%d').date()
    end_date = datetime.strptime(
        dag_run.conf['end_date'].split('T')[0].strip(), '%Y-%m-%d').date() if dag_run.conf['end_date'] else None
    current_date = pendulum.now(config.time_zone)
    
    # CHANGED: Handle supervisor assignment conditionally and only if manager exists in Replicon
    supervisor_schedule = []
    if dag_run.conf.get('manager') and bool(rail.result('search_manager_user')):
        # Only check if current supervisor is different if we have user details
        try:
            current_supervisor = rail.result('search_user_details')[0].get('manager')
            if current_supervisor != dag_run.conf['manager']:
                supervisor_schedule = [{
                    "dateRange": {
                        "startDate": {
                            "year": current_date.year,
                            "month": current_date.month,
                            "day": current_date.day
                        }
                    },
                    "item": {
                        "employeeId": dag_run.conf['manager']
                    }
                }]
        except (KeyError, IndexError, TypeError):
            # If we can't get current supervisor info, just assign the new one
            supervisor_schedule = [{
                "dateRange": {
                    "startDate": {
                        "year": current_date.year,
                        "month": current_date.month,
                        "day": current_date.day
                    }
                },
                "item": {
                    "employeeId": dag_run.conf['manager']
                }
            }]
    
    return {
        "target": {
            "employeeId": dag_run.conf['employee_id']
        },
        "modifications": {
            "firstName": {
                "value": dag_run.conf['employee_first_name']
            },
            "lastName": {
                "value": dag_run.conf['employee_last_name']
            },
            "loginName": {
                "value": dag_run.conf['email']
            },
            "emailAddress": {
                "value": dag_run.conf['email']
            },
            "employeeId": {
                "value": dag_run.conf['employee_id']
            },
            "employmentDateRange": {
                "value": {
                    "startDate": {
                        "year": start_date.year,
                        "month": start_date.month,
                        "day": start_date.day
                    },
                    "endDate": {
                        "year": end_date.year,
                        "month": end_date.month,
                        "day": end_date.day
                    } if end_date else null
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": "false" if end_date else "true"
                    },
                    "ssoName": {
                        "value": dag_run.conf['email']
                    },
                    "ssoNameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name"
                }
            },
            "locationSchedule": [
                {
                    "dateRange": {
                        "startDate": {
                            "year": current_date.year,
                            "month": current_date.month,
                            "day": current_date.day
                        }
                    },
                    "item": {
                        "name": dag_run.conf['work_location']
                    }
                }
            ] if (dag_run.conf['work_location'] != rail.result('get_effective_user_group_membership')['location']['name']) else [],
            "supervisorSchedule": supervisor_schedule  # CHANGED: Use conditional supervisor assignment
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def do_format_logs():
    log_artifacts = []
    log_records = []

    user_sync_logs = rail.result('create_user_log')

    if user_sync_logs:
        if isinstance(user_sync_logs, list):
            log_artifacts.extend(user_sync_logs)
        else:
            log_artifacts.append(user_sync_logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)
    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="add_record_count", val=len(
        list(filter(lambda x: x['action'] == 'Add', final_log_records))))
    rail.set_result(key="update_record_count", val=len(
        list(filter(lambda x: x['action'] == 'Update', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))

    return final_log_records

def get_token_data(config):
    try:
        token_data = json.loads(Variable.get(config.token_var))
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding JSON from Variable: {e}")
    
    return {
        "access_token": 'grant_type=refresh_token&refresh_token=' + token_data.get('gcxhealthcare_refresh_token') + '&client_id=' + token_data.get('gcxhealthcare_client_id') + '&client_secret=' + token_data.get('gcxhealthcare_client_secret'),
        "subscription_key": token_data.get('gcxhealthcare_subscription')
    }

def get_effective_user_group_membership_payload():
    return {
        "userUri": rail.result('search_user_details')[0]['uri']
    }
