import uuid
from datetime import datetime
import rail


def get_trigger_parallel_dagrun_conf(item, caller, project_type= None):
    return {
        "Projectname": item['Projectname'],
        "Projectcode": item['Projectcode'],
        "flag": item['flag'],
        "caller": caller,
        "get_base_project_details": rail.result('get_base_project_details'),
        "type": project_type
    }
def get_process_update_project_conf(dag_run, allocation_type):
    return {
        'Projectname': dag_run.conf['Projectname'],
        'flag': dag_run.conf['flag'],
        'ProjectURI': rail.result("load_project")['uri'],
        'Projectcode': dag_run.conf['Projectcode'],
        'Allocationtype': allocation_type,
        'Type': dag_run.conf['type']
    }

def get_process_create_project_conf(dag_run, allocation_type):
    return {
        'Projectname': dag_run.conf['Projectname'],
        'Projectcode': dag_run.conf['Projectcode'],
        'flag': dag_run.conf['flag'],
        'Allocationtype': allocation_type,
        'BaseprojectURI': dag_run.conf["get_base_project_details"]['uri'],
        'Year': dag_run.conf["get_base_project_details"]['timeEntryDateRange']['startDate']['year'],
        'Month': dag_run.conf["get_base_project_details"]['timeEntryDateRange']['startDate']['month'],
        'Day': dag_run.conf["get_base_project_details"]['timeEntryDateRange']['startDate']['day'],
        'Type': dag_run.conf['type']
    }

def get_oef_update_payload(dag_run, project_type):
    oef_name = 'FTE' if not dag_run.conf['Type'] else 'Consultant'
    return {
        "objectUri": rail.result('load_project')['uri'] if project_type == 'create' else dag_run.conf['ProjectURI'],
        "value": {
            "definition": {
            "uri": rail.result("get_all_oefs")[0]['uri'],
            },
            "tag": {
                "uri": rail.find_first_by_attr_and_get_attr(rail.result("get_oef_drop_down_values_eligibility")['tags'], "name", oef_name, "uri"),
            }
        }
    }

def get_update_project_payload(dag_run):
    return {
            "target": {
                "uri": dag_run.conf['ProjectURI']
            },
            "modifications": {
                "nameToApply": {
                "value": dag_run.conf['Projectname']
                },
                "endDateToApply": {
                "date": None if dag_run.conf['flag'].upper() !='N' else {
                    'year': datetime.now().strftime("%Y"),
                    'month': datetime.now().strftime("%m"),
                    'day': datetime.now().strftime("%d")
                }
                },
                "statusToApply": {
                "name": "In Progress" if dag_run.conf['flag'].upper() !='N' else "Completed"
                }
            },
            "programToApply": {
                "program": {
                "name": dag_run.conf['Allocationtype']
                }
            },
            "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
            "unitOfWorkId": str(uuid.uuid4())
        }

def get_create_project_payload(dag_run):
    return {
            "copyParameter": {
                "sourceProject": {
                "uri": dag_run.conf['BaseprojectURI']
                },
                "destinationProjectInfo": {
                "name": dag_run.conf['Projectname'],
                "code": dag_run.conf['Projectcode'] if not dag_run.conf['Type'] else dag_run.conf['Projectcode'] + '_',
                "dateRange": {
                    "startDate": {
                    "year": dag_run.conf['Year'],
                    "month": dag_run.conf['Month'],
                    "day": dag_run.conf['Day']
                    },
                    "endDate": None
                },
                "statusLabel": {
                    "name": "In Progress" if dag_run.conf['flag'].upper() !='N' else "Completed"
                },
                "program": {
                    "name": dag_run.conf['Allocationtype']
                }
                },
                "taskCopyOptionUri": "urn:replicon:project-copy-task-copy-option:copy",
                "teamCopyOptionUri": "urn:replicon:project-copy-team-copy-option:copy",
                "billingRateCopyOptionUri": "urn:replicon:project-copy-billing-rate-copy-option:copy-from-project",
                "expenseCodeCopyOptionUri": "urn:replicon:project-copy-expense-code-copy-option:do-not-copy",
                "taskDateCopyOptionUri": "urn:replicon:task-date-copy-option:copy-date",
                "shiftDatesByProjectStartDateOffset": "true"
            }
        }
