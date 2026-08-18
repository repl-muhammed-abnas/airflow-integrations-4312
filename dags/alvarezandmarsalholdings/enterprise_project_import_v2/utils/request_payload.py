# pylint:disable = too-many-statements
import uuid
from functools import lru_cache
import rail

null = None
DATE_FORMAT = "%Y-%m-%d"

MANDATORY_FIELDS = {
    "project_fields": {
        "Project":"Project",
    },
    "task_fields": {
        "ProjectElement":"ProjectElement",
        "ProjectElementDescription": "ProjectElementDescription",
    }

    }

PROJECT_STATUS= {'10': 'In Progress'}
TASKS_STATUS= {'10': 'Open'}

def get_all_mandatory_check_projects(dag_run):
    for _, value in MANDATORY_FIELDS['project_fields'].items():
        if not dag_run.conf[value]:
            return False
    return True

def get_exception_message(dag_run, mandatory_fields):
    missing_fields = []
    for payload_key, log_value in mandatory_fields.items():
        if not dag_run.conf[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")


def get_replicon_date(input_date):
    return rail.parse_date(input_date, DATE_FORMAT)

def get_cost_center_payload():
    return {
        "page": 1,
        "pagesize": 200,
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center",
            "urn:replicon:cost-center-list-column:effectively-enabled",
            "urn:replicon:cost-center-list-column:code",
        ],
        "sort": [],
        "filterExpression": null
    }

def get_users_payload():
    return {
        "page": 1,
        "pagesize": 200,
        "columnUris": [
            "urn:replicon:user-list-column:user-name",
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled",
        ],
        "sort": [],
        "filterExpression": null
    }

@lru_cache(maxsize=8)
def get_cost_center_and_user_details_json_artifact():
    return {
        'get_all_costcenters': rail.write_json_artifact(rail.result('get_all_costcenters')),
        'get_all_users_data': rail.write_json_artifact(rail.result('get_all_users'))
    }

@lru_cache(maxsize=8)
def get_specific_permissionsets():
    return rail.result('get_permission_sets')

def get_payload_item_with_artifacts(item):
    tasks = item.get('A_EnterpriseProjectElementType')
    tasks = tasks if isinstance(tasks, list) else []
    return {
        "Project": item.get("Project", ''),
        "ProjectDescription": item.get("ProjectDescription", ''),
        "ProjectProfileCode": item.get("ProjectProfileCode", ''),
        "ProcessingStatus": item.get("ProcessingStatus", ''),
        "ResponsibleCostCenter": item.get("ResponsibleCostCenter", ''),
        "ProjectStartDate": item.get("ProjectStartDate", ''),
        "ProjectEndDate": item.get("ProjectEndDate", ''),
        "ControllingArea": item.get("ControllingArea", ''),
        "EntProjTimeRecgIsBlkd": item.get("EntProjTimeRecgIsBlkd", ''),
        "ProjectManager": item.get("ProjectManager", ''),
        "A_EnterpriseProjectElementType": rail.write_json_artifact(tasks)
    }

def get_process_project_conf(item, config):

    return {
        **get_payload_item_with_artifacts(item),
        **get_cost_center_and_user_details_json_artifact(),
        **get_specific_permissionsets(),
        **{
            "cost_center_uri": rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_costcenters'), 'code',item['ResponsibleCostCenter'], 'uri'
            ),
            'get_project_profile_oef_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_project_oef_details'), 'name',
                config.PROJECT_PROFILE, 'uri'
            ),
            'project_profile_tag_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_project_profile_oef_details'), 'name',
                item['ProjectProfileCode'], 'uri'
            ),
            'get_controlling_area_oef_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_project_oef_details'), 'name',
                config.CONTROLLING_AREA, 'uri'
            ),
            'get_billing_responsible_oef_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_task_oef_details'), 'name',
                config.BILLING_RESPONSIBLE, 'uri'
            ),
            'exception_log' : rail.result('create_exception_log')
        }
    }


def does_project_code_exist():
    return bool(rail.result('get_project_details'))

def get_create_project_target_param():
    if does_project_code_exist():
        return {
            "uri": rail.result('get_project_details')['uri']
        }
    return None

def assign_permission_set(dag_run):
    return {
        "userUri": rail.result('get_user_info_on_empid')['uri'],
        "permissionSetUri": dag_run.conf['project_manager_permissionuri']
    }


def get_time_and_expense_entry(task):
    return "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"

def get_projectleadertoapply_param():
    is_project_manager_present = rail.result('log_project_manager_present_and_enabled') or ''
    if is_project_manager_present == 'Present':
        if not does_project_code_exist():
            return {
                    "user": {
                        "uri": rail.result('get_user_info_on_empid')['uri']
                    }
                }
        if does_project_code_exist() and (not rail.result('get_project_details')['projectLeader'] or \
            rail.result('get_project_details')['projectLeader']['uri'] != rail.result('get_user_info_on_empid')['uri']):
            return {
                "user": {
                    "uri": rail.result('get_user_info_on_empid')['uri']
                }
            }
    return null


def get_updated_cost_center(dag_run):

    if not does_project_code_exist():
        return {
            "costCenter": {
                "uri": dag_run.conf['cost_center_uri'],
                "parentUri": null,
                "name": null
            }
        }
    elif not (rail.result('get_project_details')['costCenter']) or (dag_run.conf['cost_center_uri'] != rail.result('get_project_details')['costCenter']['uri']):
        return {
            "costCenter": {
                "uri": dag_run.conf['cost_center_uri'],
                "parentUri": null,
                "name": null
            }
        }
    return null

def create_or_update_project_payload(dag_run, config):
    
    extension_fields = rail.result("get_project_details")['extensionFieldValues'] if rail.result("get_project_details") else []
    
    def get_oef_fields_definitions():
        oef_list = []

        def check_existing_oef_value(oef_id, oef_value='', text_value=True):
            
            if not does_project_code_exist():
                return False
            if not extension_fields:
                return False
            if text_value:
                oef_text_val = rail.find_first_by_attr_and_get_attr(extension_fields, 'definition.displayText', oef_id, 'textValue')
                return bool(oef_text_val == oef_value)
            else:
                tag_value = rail.find_first_by_attr_and_get_attr(extension_fields, 'definition.displayText', oef_id, 'tag.displayText')
                return bool(tag_value == oef_value)

        def get_oef_payload(oef_uri, _text_value=None, _dropdown_value=None):
            
            if oef_uri:
                oef_list.append(
                        {
                    "definition": {
                        "uri": oef_uri
                    },
                    "textValue": _text_value,
                    "tag": {
                        "uri": _dropdown_value,
                    } if _dropdown_value else None
                })

        if dag_run.conf['ControllingArea'] and not check_existing_oef_value(config.CONTROLLING_AREA, dag_run.conf['ControllingArea']):
            get_oef_payload(dag_run.conf['get_controlling_area_oef_uri'], 
                            _text_value= dag_run.conf['ControllingArea'])

        if dag_run.conf['ProjectProfileCode'] and not check_existing_oef_value(config.PROJECT_PROFILE, dag_run.conf['ProjectProfileCode'], False):
            get_oef_payload(dag_run.conf['get_project_profile_oef_uri'], _dropdown_value=dag_run.conf[
                'project_profile_tag_uri'])

        return oef_list

    def get_is_timeentry_allowed():
        return 'false'

    modifications = {
        "nameToApply": {
            "value": dag_run.conf["ProjectDescription"]
        },
        "codeToApply":  {
            "value": dag_run.conf["Project"]
        } if not does_project_code_exist() else None,
        "startDateToApply": {
            "date": get_replicon_date(dag_run.conf['ProjectStartDate'])
        } if dag_run.conf['ProjectStartDate'] else null,
        "endDateToApply": {
            "date": get_replicon_date(dag_run.conf['ProjectEndDate'])
        } if dag_run.conf['ProjectEndDate'] else null,
        "statusToApply": {
            "name": PROJECT_STATUS.get(dag_run.conf['ProcessingStatus'], 'Completed')
        },
        "clientAssignmentsSchedulesToApply": null,
        "projectLeaderToApply": get_projectleadertoapply_param(),
        "isTimeEntryAllowed": get_is_timeentry_allowed(),
        "costCenterToApply": get_updated_cost_center(dag_run) if dag_run.conf['cost_center_uri'] else null,
        "timeAndMaterials": null,
        "objectExtensionFieldsToApply": get_oef_fields_definitions(),
        "locationToApply": null,
        "keyValuesToApply": [],
        "customFieldsToApply": [],
    }

    return {
        "target": get_create_project_target_param(),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

@lru_cache(maxsize=8)
def get_project_log_details(dag_run):
    pm_note = rail.result('log_project_manager_disabled_or_not_present') or ''
    pm_available = rail.result('log_project_manager_not_present') or ''
    
    details = ["Project Updated" if does_project_code_exist() else "Project Added"]
    partial = False

    if pm_note:
        details.append(pm_note)
        partial = True
    
    if pm_available:
        details.append(pm_available)
        partial = True

    for key, label in [('ProjectProfileCode', 'Project Profile Code')]:
        if not dag_run.conf.get(key):
            details.append(f"{label} is not present in payload")
            partial = True
    if not dag_run.conf['cost_center_uri']:
        details.append(f"ResponsibleCostCenter {dag_run.conf['ResponsibleCostCenter']} is disabled or not present in Replicon")
        partial = True

    return {
        "message": rail.smartjoin_by_delim(details, ';'),
        "status": "Success" if not partial else "Exception"
    }

def get_updated_task_details(level):
    current_task_in_project = rail.result('get_all_tasks_for_project')
    def get_updated_tasks(task_id):
        if rail.result(task_id):
            return rail.result(task_id)['existing_tasks']
        return null
    current_task_in_project = get_updated_tasks('add_task_3') or get_updated_tasks('add_task_2') or get_updated_tasks('add_task_1') or get_updated_tasks('add_task_1parent_in_system') or current_task_in_project
    return current_task_in_project

def get_parent_task_uri(task, level):
    existing_tasks = get_updated_task_details(level)
    parent_task = existing_tasks.get(task['parent_task'], {})
    return parent_task.get('uri')

def get_task_target(action,task,project_uri, level):
    if action == "add" and not task['parent_task']:
        return null
    parent_task_uri = get_parent_task_uri(task, level)
    return {
        "uri": None if action == "add" else task['uri'],
        "parent": {
            "uri": parent_task_uri
        } if parent_task_uri and action == "add" else null
    }

@lru_cache(maxsize=8)
def get_billing_users_data():
    return rail.result('get_billing_responsible_users_data') or []

def get_task_oef_fields_definitions(dag_run, task):
    oef_list = []

    def get_oef_payload(oef_uri, _textvalue=null, _dropdown_value=null):
        if oef_uri:
            oef_list.append(
            {
                "definition": {
                    "uri": oef_uri
                },
                "textValue": _textvalue,
                "tag": {
                    "uri": _dropdown_value,
                } if _dropdown_value else None
            }
        )
    users_data = get_billing_users_data()
    existing_tasks = get_updated_task_details(level=3)
    user = rail.find_first_by_attr_and_get_attr(
        users_data, 'employeeid', task['billing_responsible']
    )
    if user and task['billing_responsible']:
        get_oef_payload(dag_run.conf['get_billing_responsible_oef_uri'], _textvalue=task['billing_responsible'])
    # else:
    #     task_data = existing_tasks.get(task['taskcode'], {})
    #     if task_data and task_data.get('billing_resp', '') and not task['billing_responsible']:
    #         get_oef_payload(dag_run.conf['get_billing_responsible_oef_uri'], _textvalue=task['billing_responsible'])
    return oef_list

def get_is_task_closed(task):
    task_status = task['task_status']
    return 0 if task_status in ['10', 10] else 1

def get_replicon_task_date(task, input_date):
    input_date = task.get(input_date)
    if input_date:
        return rail.parse_date(input_date, DATE_FORMAT)
    return null

def get_is_timeentry_allowed(task):
    allow_time_entry = task['allow_time_entry']
    return 0 if allow_time_entry == 'X' else 1

def get_task_payload(dag_run, action,data,project_uri, level):
    return list(map(lambda task: {
        "target": get_task_target(action,task,project_uri, level),
        "taskModificationToApply": {
                "name": task['taskname'],
                "codeToApply": {
                    "value": task['taskcode']
                }if action == "add" else null,
                "isClosed": get_is_task_closed(task),
                "timeAndExpenseEntryTypeToApply": {
                    "value": get_time_and_expense_entry(task)
                },
                "isTimeEntryAllowed": get_is_timeentry_allowed(task),
                "timeEntryStartDateToApply": {
                    "date": startdate
                } if (startdate := get_replicon_task_date(task, 'startdate')) else null,
                "timeEntryEndDateToApply": {
                    "date": enddate
                } if (enddate := get_replicon_task_date(task, 'enddate')) else null,
                "objectExtensionFieldsToApply": get_task_oef_fields_definitions(dag_run, task)
            }
    }, data))

def get_update_task_payload(dag_run, data, level):
    project_uri = rail.result('update_project')['uri'] if does_project_code_exist() else rail.result('create_project')['uri']
    return {
        "project": {
            "uri": project_uri,
        },
        "taskHierarchy": get_task_payload(dag_run, "update",
                                          data,
                                          project_uri, level),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_add_task_payload(dag_run, data, level):
    project_uri = rail.result('update_project')['uri'] if does_project_code_exist() else rail.result('create_project')['uri']
    return {
        "project": {
            "uri": project_uri,
        },
        "taskHierarchy": get_task_payload(dag_run, "add",
                                          data,
                                          project_uri, level),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }



def get_billing_responsible_users_payload():
    billing_responsibles = rail.load_all_records(rail.result('format_payload_tasks'))[0]['billing_responsibles']
    users = [
        {
            "employeeId": empl_id[0]
        } for empl_id in billing_responsibles
    ]
    return {
        "users": users,
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }
