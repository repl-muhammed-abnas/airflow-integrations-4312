# pylint:disable = too-many-statements
import uuid
from functools import lru_cache
import rail

null = None
DATE_FORMAT = "%Y-%m-%d"

MANDATORY_FIELDS = {
    "project_fields": {
        "ProjectID":"ProjectID",
    },
    "task_level1_fields": {
        "WorkPackageID":"WorkPackageID",
        "WorkPackageName": "WorkPackageName",
    },
    "task_level2_fields": {
        "Workitem":"Workitem",
        "Workitemname": "Workitemname",
        "WorkpackageID": "WorkpackageID",
    },

    }

PROJECT_STATUS= {'P003': 'In Progress', 'P004': 'Completed', 'P005': 'Completed'}

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


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

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

def get_process_project_conf(item, config):

    return {
        **item,
        **get_cost_center_and_user_details_json_artifact(),
        **get_specific_permissionsets(),
        **{
            "cost_center_uri": rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_costcenters'), 'code',item['CostCenter'], 'uri'
            ),
            'get_project_profile_oef_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_project_oef_details'), 'name',
                config.PROJECT_PROFILE, 'uri'
            ),
            'project_profile_tag_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_project_profile_oef_details'), 'name',
                item['ProjectProfileCode'], 'uri'
            ),
            'get_federal_project_oef_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_project_oef_details'), 'name',
                config.FEDERAL_PROJECT, 'uri'
            ),
            'federal_project_tag_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_federal_project_oef_details'), 'name',
                item['FederalProject'], 'uri'
            ),
            'get_controlling_area_oef_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_project_oef_details'), 'name',
                config.CONTROLLING_AREA, 'uri'
            ),

            'get_billing_control_category_oef_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_task_oef_details'), 'name',
                config.BILLING_CONTROL_CATEGORY, 'uri'
            ),
            'get_billing_responsible_oef_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_task_oef_details'), 'name',
                config.BILLING_RESPONSIBLE, 'uri'
            ),
            'get_work_package_code_oef_uri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_task_oef_details'), 'name',
                config.WORK_PACKAGE_CODE, 'uri'
            ),
            'exception_log' : rail.result('create_exception_log')
        }
    }

def get_resource_assignment_payload():
    return {
        "taskUris": [item['uri'] for item in rail.result('get_all_task_details_for_project')],
        "asOfDate": null
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
    non_billable_wp = task.get('non_bill_wp') if not task.get('parent') else task['parent']['non_bill_wp']
    if not non_billable_wp:
        return "urn:replicon:time-and-expense-entry-type:billable"
    if non_billable_wp == 'X':
        return "urn:replicon:time-and-expense-entry-type:non-billable"
    return "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"

def get_projectleadertoapply_param():
    if get_task_state('log_project_manager_present_and_enabled') == 'success':
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
    # can be used for future CRs
    def get_client_payload():
        return None
    
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

        if dag_run.conf['FederalProject'] and not check_existing_oef_value(config.FEDERAL_PROJECT, dag_run.conf['FederalProject'], False):
            get_oef_payload(dag_run.conf['get_federal_project_oef_uri'],_dropdown_value=dag_run.conf[
                'federal_project_tag_uri'])

        return oef_list

    modifications = {
        "nameToApply": {
            "value": dag_run.conf["ProjectName"]
        },
        "codeToApply":  {
            "value": dag_run.conf["ProjectID"]
        } if not does_project_code_exist() else None,
        "startDateToApply": {
            "date": get_replicon_date(dag_run.conf['StartDate'])
        } if dag_run.conf['StartDate'] else null,
        "endDateToApply": {
            "date": get_replicon_date(dag_run.conf['EndDate'])
        } if dag_run.conf['EndDate'] else null,
        "statusToApply": {
            "name": PROJECT_STATUS.get(dag_run.conf['ProjectStage'], 'In Progress')
        },
        "clientAssignmentsSchedulesToApply": get_client_payload(),
        "projectLeaderToApply": get_projectleadertoapply_param(),
        "isTimeEntryAllowed": "false",
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

    for key, label in [('ProjectProfileCode', 'Project Profile Code'), ('FederalProject', 'Federal Project')]:
        if not dag_run.conf.get(key):
            details.append(f"{label} is not present in payload")
            partial = True
    if not dag_run.conf['cost_center_uri']:
        details.append(f"CostCenter {dag_run.conf['CostCenter']} is disabled or not present in Replicon")
        partial = True

    return {
        "message": rail.smartjoin_by_delim(details, ';'),
        "status": "Success" if not partial else "Exception"
    }

def get_task_target(action,task,project_uri):
    if action == "add" and not task['parent']:
        return null
    return {
        "uri": None if action == "add" else task['uri'],
        "parent": {
          "name": task['parent']['taskname'],
          "project": {
            "uri": project_uri
          }
        } if task['parent'] and action == "add" else null
    }


@lru_cache(maxsize=8)
def get_billing_users_data():
    return rail.result('get_billing_responsible_users_data') or []


def get_task_oef_fields_definitions(dag_run, task):
    oef_list = []

    def get_oef_payload(oef_uri, _text_value=None, _dropdown_value=None):
        if not oef_uri:
            return None
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

    non_billable_wp = task.get('non_bill_wp') if not task.get('parent') else task['parent']['non_bill_wp']
    _text_value = ''
    if non_billable_wp == 'X':
        _text_value = 'NON_BILL'
    get_oef_payload(dag_run.conf['get_billing_control_category_oef_uri'], _text_value=_text_value)
    billing_responsible = task.get('billing_responsible') if not task.get('parent') else task['parent']['billing_responsible']
    users_data = get_billing_users_data()
    if billing_responsible and users_data:
        user = rail.find_first_by_attr_and_get_attr(
            users_data, 'employeeid', billing_responsible
        )
        if user:
            get_oef_payload(dag_run.conf['get_billing_responsible_oef_uri'], _text_value=billing_responsible)
    workpackageid = task.get('taskcode') if not task.get('parent') else task['parent']['taskcode']
    get_oef_payload(dag_run.conf['get_work_package_code_oef_uri'], _text_value=workpackageid)
    return oef_list

def get_is_task_closed(task):
    work_package_func_is_blocked = task.get('work_package_func_is_blocked') if not task.get('parent') else task['parent']['work_package_func_is_blocked']
    return 1 if work_package_func_is_blocked == 'X' else 0

def get_replicon_task_date(task, input_date):
    input_date = task.get(input_date) if not task.get('parent') else task['parent'][input_date]
    if input_date:
        return rail.parse_date(input_date, DATE_FORMAT)
    return null

def get_task_payload(dag_run, action,data,project_uri):
    return list(map(lambda task: {
        "target": get_task_target(action,task,project_uri),
        "taskModificationToApply": {
                "name": task['taskname'],
                "codeToApply": {
                    "value": task['taskcode']
                }if action == "add" else null,
                "isClosed": get_is_task_closed(task),
                "timeAndExpenseEntryTypeToApply": {
                    "value": get_time_and_expense_entry(task)
                },
                "isTimeEntryAllowed": "1",
                "timeEntryStartDateToApply": {
                    "date": startdate
                } if (startdate := get_replicon_task_date(task, 'startdate')) else null,
                "timeEntryEndDateToApply": {
                    "date": enddate
                } if (enddate := get_replicon_task_date(task, 'enddate')) else null,
                "objectExtensionFieldsToApply": get_task_oef_fields_definitions(dag_run, task)
            }
    }, data))

def get_update_task_payload(dag_run, data):
    project_uri = rail.result('update_project')['uri'] if does_project_code_exist() else rail.result('create_project')['uri']
    return {
        "project": {
            "uri": project_uri,
        },
        "taskHierarchy": get_task_payload(dag_run, "update",
                                          data,
                                          project_uri),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_add_task_payload(dag_run, data):
    project_uri = rail.result('update_project')['uri'] if does_project_code_exist() else rail.result('create_project')['uri']

    return {
        "project": {
            "uri": project_uri,
        },
        "taskHierarchy": get_task_payload(dag_run, "add",
                                          data,
                                          project_uri),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }



def get_billing_responsible_users_payload():
    billing_responsibles = rail.result('format_payload_tasks')['billing_responsibles']
    users = [
        {
            "employeeId": empl_id[0]
        } for empl_id in billing_responsibles
    ]
    return {
        "users": users,
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }
