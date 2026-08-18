from datetime import datetime as dt
import uuid
import rail
from rail import get_current_context

def does_wbs_exist():
    return bool(rail.result('get_project_details'))

def get_task_state(task_id):
    task_instance = get_current_context()['dag_run'].get_task_instance(task_id)
    return task_instance.current_state() if task_instance else None

def get_log_message():
    msg =''
    if get_task_state("log_user_skipped").lower() == "success":
        msg += ', project manager is not synced since the user details are not received from the payload'

    if does_wbs_exist():
        return "Project Updated Successfully" + msg
    return "Project Added Successfully" + msg

def get_create_project_target_param():
    if does_wbs_exist():
        return {
            "uri": rail.result('get_project_details')['uri']
        }
    return None

def create_projectorapply_modifications(dag_run):
    get_project_data = rail.result("load_project_data_from_query")
    modifications = {
        "nameToApply": {
            "value": get_project_data["projectname"]
        },
        "codeToApply":  {
            "value": get_project_data["projectcode"]
        } if not does_wbs_exist() else None,
        "descriptionToApply": {
            "value": get_project_data["projectname"]
        } if get_project_data["projectname"] else None,
        "billingTypeToApply": {
            "value": "urn:replicon:billing-type:time-and-material"
        },
        "startDateToApply": {
            "date": rail.parse_date(get_project_data["projectstartdate"],"%Y%m%d")
        } if get_project_data["projectstartdate"] else None,
        "endDateToApply": {
            "date": rail.parse_date(get_project_data["projectenddate"],"%Y%m%d")
        } if get_project_data["projectenddate"] else None,
        "timeAndMaterials": {
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
            "billingRates": [{
                "billingRate": {
                    "uri": "urn:replicon:project-specific-billing-rate"
                }
            }]
        },
        "statusToApply": {
            "name": 'In Progress'
        },
        "isTimeEntryAllowed": "0",
        "customFieldsToApply": [
            {
                "customField": {
                    "uri": dag_run.conf['last_modified_date_udf_uri']
                },
                "date": rail.get_replicon_date(dt.now())
            }
        ],
        "objectExtensionFieldsToApply": [
            {
                "definition": {
                    "uri": dag_run.conf['project_export_type_oef'],
                },
                "tag": {
                    "uri": dag_run.conf['it_proj_details_dropdown_uri']
                }
            },
            
        ] if not does_wbs_exist() else []
    }

    return {
        "target": get_create_project_target_param(),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_create_user_payload(dag_run):
    user_data = rail.result("load_project_data_from_query")
    return {
        "user": {
            "target": {
                "loginName": user_data["pm_loginname"]+'@wipro.com'
            },
            "firstname": user_data['pm_name'].split(' ')[0],
            "lastname": user_data['pm_name'][len(user_data['pm_name'].split(' ')[0])+1:],
            "emailAddress": user_data['pm_email'],
            "employeeId": user_data['pm_empid'],
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "1",
                "loginName": user_data["pm_loginname"]+'@wipro.com',
                "SSOName": user_data["pm_loginname"]+ '@wipro.com',
            },
            "employeeType": {
                "uri": dag_run.conf["employeetypeuri"]
            } if dag_run.conf["employeetypeuri"] else None,
            "permissionSets": [
                {
                    "name": 'Project Manager'
                },
                {
                    "name": 'L1 Manager'
                },
                {
                    "name": 'End User (Managers)'
                }
            ],
        }
    }
