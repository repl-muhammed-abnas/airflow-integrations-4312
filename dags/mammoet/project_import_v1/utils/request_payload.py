from datetime import datetime
import uuid
import rail

DATE_FORMAT = '%Y-%m-%d'

def get_all_mandatory_check_projects(dag_run):
    if dag_run.conf['projecttype']:
        if dag_run.conf['projecttype'] == 'WBS':
            return dag_run.conf['programcode'] and dag_run.conf['programexternalcode'] and dag_run.conf['programname'] and \
            dag_run.conf['programstatus'] and dag_run.conf['projectcode'] and dag_run.conf['projectexternalcode'] and dag_run.conf['projectname'] and \
                dag_run.conf['projectstatus'] and dag_run.conf['projectmanager'] and dag_run.conf['clientcode'] and dag_run.conf['clientname']
        if dag_run.conf['projecttype'] == 'PM Order':
            return dag_run.conf['projectcode'] and dag_run.conf['projectexternalcode'] and dag_run.conf['projectname'] and \
                dag_run.conf['projectstatus'] and dag_run.conf['projectmanager']
    return False

mandatory_fields = {
    "wbs_project_fields": {
        "programcode": "programcode",
        "programexternalcode": "programexternalcode",
        "programname": "programname",
        "programstatus": "programstatus",
        "projectcode": "projectcode",
        "projectexternalcode": "projectexternalcode",
        "projectname": "projectname",
        "projectstatus": "projectstatus",
        "projectmanager": "projectmanager",
        "clientcode": "clientcode",
        "clientname": "clientname",
        "projecttype": "projecttype"
    },
    "pmo_project_fields": {
        "projectcode": "projectcode",
        "projectexternalcode": "projectexternalcode",
        "projectname": "projectname",
        "projectstatus": "projectstatus",
        "projectmanager": "projectmanager",
        "projecttype": "projecttype"
    },
    "task_fields": {
        "projectcode":"projectcode",
        "taskcode": "taskcode",
        "taskname":"taskname",
        "taskstatus": "taskstatus"
    }
    }

def get_invalid_task_logs(item):
    def get_missing_field():
        not_present_fields = []
        for field in mandatory_fields['task_fields']:
            if item[field] in [None, '']:
                not_present_fields.append(field)
        not_present_fields = list(filter(None, not_present_fields))
        return ";".join(not_present_fields)
    return {
        "projectcode": item['projectcode'],
        "taskcode": item['taskcode'],
        "taskname": item['taskname'],
        "action": 'Add',
        "details": get_missing_field() + " not present in the input",
        "status": 'Skipped'
    }

def get_invalid_project_type(item):
    return {
        "projectcode": item['projectcode'],
        "projectname(code)": item['projectexternalcode'],
        "projectname(name)": item['projectname'],
        "programcode": item['programcode'],
        "programname(code)": item['programexternalcode'],
        "programname(name)": item['programname'],
        "clientcode": item['clientcode'],
        "clientname": item['clientname'],
        "projecttype": item['projecttype'],
        "details": "project type is not allowed/blank",
        "status": 'Skipped'
    }

def get_invalid_logs_property_conf(item):
    _type = 'wbs_project_fields' if item['projecttype'] == "WBS" else 'pmo_project_fields'
    def get_missing_field():
        not_present_fields = []
        for field in mandatory_fields[_type]:
            if item[field] in [None, '']:
                not_present_fields.append(field)
        not_present_fields = list(filter(None, not_present_fields))
        return ";".join(not_present_fields)
    return {
        "projectcode": item['projectcode'],
        "projectname(code)": item['projectexternalcode'],
        "projectname(name)": item['projectname'],
        "programcode": item['programcode'],
        "programname(code)": item['programexternalcode'],
        "programname(name)": item['programname'],
        "clientcode": item['clientcode'],
        "clientname": item['clientname'],
        "projecttype": item['projecttype'],
        "details": get_missing_field() + " not present in feed file",
        "status": 'Skipped'
    }

def does_wbs_exist():
    return bool(rail.result('get_project_details'))

def get_create_project_target_param():
    if does_wbs_exist():
        return {
            "uri": rail.result('get_project_details')['uri']
        }
    return None

def create_projectorapply_modifications(dag_run):
    def get_client_payload():
        return {
            "clients": [
                {
                "client": {
                    "name": dag_run.conf['clientname'],
                },
                "costAllocationPercentage": "100"
                }
            ],
            "effectiveDate": None
        }

    def get_program_payload():
        return {
            "program": {
                "name": f'{dag_run.conf["programname(code)"]} {dag_run.conf["programname(name)"]} ({dag_run.conf["programcode"]})'
            }
        }

    modifications = {
        "nameToApply": {
            "value": dag_run.conf["projectname(code)"] +' '+ dag_run.conf["projectname(name)"]
        },
        "codeToApply":  {
            "value": dag_run.conf["projectcode"]
        } if not does_wbs_exist() else None,
        "startDateToApply": {
            "date": get_dates_param(dag_run.conf['projectstartdate'],DATE_FORMAT)
        },
        "endDateToApply": {
            "date": get_dates_param(dag_run.conf['projectenddate'],DATE_FORMAT),
        },
        "clientAssignmentsSchedulesToApply": get_client_payload() if dag_run.conf["projecttype"] == 'WBS' else None,
        "statusToApply": {
            "name": "In Progress" if 'In' in dag_run.conf['projectstatus'] else dag_run.conf['projectstatus']
        },
        "isProjectLeaderApprovalRequired": '0' if dag_run.conf["projecttype"] != 'WBS' else '1',
        "programToApply": get_program_payload() if dag_run.conf["projecttype"] == 'WBS' else None,
        "projectLeaderToApply": {
            "user": {
                "employeeId": dag_run.conf["projectmanager"]
            }
        }if rail.result("get_user_details") else None,
        "resourceAssignmentModifications": {
        "resourcesToAdd": [
            {
                "department": {
                    "uri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1"
                }
            }
        ]
        }
    }

    return {
        "target": get_create_project_target_param(),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_dates_param(date,_format,_repl_date = None):
    return rail.parse_date(date,_format) if date else rail.parse_date(_repl_date,_format) if _repl_date else None

def get_put_program_param():
    data = rail.result("get_query_data")
    return {
        "program": {
            "target": {
                "name": f"{data['programexternalcode']} {data['programname']} ({data['programcode']})"
            },
            "name": f"{data['programexternalcode']} {data['programname']} ({data['programcode']})",
            "dateRange": {
                "startDate": get_dates_param(data['programstartdate'],DATE_FORMAT),
                "endDate": get_dates_param(data['programenddate'],DATE_FORMAT)
            },
            "isActive": (data['programstatus'] == 'Active')
        }
    }

def get_create_client_payload():
    return {
        "client": {
            "target": {
                "name": rail.result("get_query_data")['clientname']
            },
            "name": rail.result('get_query_data')['clientname'],
            "code": rail.result('get_query_data')['clientcode'],
            "isActive": True
        }
    }

def get_program_daterange_param():
    data = rail.result("get_program_details")['dateRange']
    start_date = f"{data['startDate']['year']}-{data['startDate']['month']:02d}-{data['startDate']['day']:02d}" if data['startDate'] else None
    end_date = f"{data['endDate']['year']}-{data['endDate']['month']:02d}-{data['endDate']['day']:02d}" if data['endDate'] else None
    return {
        "programUri": rail.result("search_program_in_replicon")[0],
        "dateRange": {
            "startDate": get_dates_param(rail.result("get_query_data")['programstartdate'],DATE_FORMAT,start_date),
            "endDate": get_dates_param(rail.result("get_query_data")['programenddate'],DATE_FORMAT,end_date)
        }
    }

def get_task_payload(action,data):
    return list(map(lambda task: {
        "target": None if action == "add" else {"uri": task['uri']},
        "taskModificationToApply": {
                "name": task['taskcode'],
                "codeToApply": {
                    "value": task['taskname'].strip()[:50]
                },
                "isClosed": not (task['taskstatus'] == 'Open'),
                "timeEntryStartDateToApply": {
                    'date': get_dates_param(task['taskstartdate'],DATE_FORMAT)
                },
                "timeEntryEndDateToApply": {
                    'date': get_dates_param(task['taskenddate'],DATE_FORMAT)
                },
                "timeAndExpenseEntryTypeToApply": {
                    "value": "urn:replicon:time-and-expense-entry-type:billable"
                },
                "isTimeEntryAllowed": "1",
                "resourceAssignmentModifications": {
                "resourcesToAdd": [
                    {
                        "department": {
                            "uri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1"
                        }
                    }
                ]
                },
            }
    }, data))

def is_enddate_less_than_today(end_date, today, fmt=None):
    return datetime.fromisoformat(today) <= datetime.strptime(
        f"{end_date['year']}-{end_date['month']}-{end_date['day']}", fmt) if end_date else True

def get_batch_put_task_payload():
    return {
        "project": {
            "uri": rail.result('get_project_details')['uri'],
        },
        "taskHierarchy": get_task_payload("add",rail.result("get_all_task_to_add_update")['tasks_to_add']),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_update_task_payload():
    return {
        "project": {
            "uri": rail.result('get_project_details')['uri'],
        },
        "taskHierarchy": get_task_payload("update",rail.result("get_all_task_to_add_update")['tasks_to_update']),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_oef_update_payload(dag_run):
    return {
        "objectUri": rail.result('create_projectorapply_modifications')['uri'],
        "value": {
            "definition": {
            "uri": dag_run.conf['project_type_uri'],
            },
            "tag": {
                "uri": dag_run.conf['project_type_definition_uri'],
            }
        }
    }
