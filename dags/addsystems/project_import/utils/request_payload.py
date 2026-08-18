from datetime import datetime
import uuid
import rail
from rail import get_current_context
from rail.lib.ecid import get_dagrun_ecid
null = None


def get_task_state(task_id):
    task_instance = get_current_context()['dag_run'].get_task_instance(task_id)
    return task_instance.current_state() if task_instance else null


def mandatory_fields_check(dag_run):
    return (dag_run.conf['item']['ClienteleCallNum'] and dag_run.conf['item']['ClienteleCallName'] and dag_run.conf['item']['ClienteleCallSummary']
            and dag_run.conf['item']['CallOpenDate'])


def update_project_create_or_modifiy(dag_run):
    return {
        "target": {
            "uri": rail.result('get_project_details')['uri']
        },
        "modifications": {
            "nameToApply": {
                "value": dag_run.conf['item']['ClienteleCallName']
            },
            "codeToApply": {
                "value": dag_run.conf['item']['ClienteleCallNum']
            },
            "descriptionToApply": {
                "value": dag_run.conf['item']['ClienteleCallSummary']
            },
            "clientBillingAllocationMethodToApply": "urn:replicon:client-billing-allocation-method:user-specified",
            "isTimeEntryAllowed": "false",
            "clientAssignmentsSchedulesToApply": {
                "clients":
                client_for_each(dag_run)
            },
            "resourceProjectAssignmentModifications": {
                "resourcesToAdd": [
                    {
                        "resource": {
                            "department": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":department:1"
                            }
                        }
                    }
                ]
            }
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def create_project_create_or_modifiy(dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['item']['CallOpenDate'].split('T')[0].strip(), '%Y-%m-%d').date()
    return {
        "modifications": {
            "nameToApply": {
                "value": dag_run.conf['item']['ClienteleCallName']
            },
            "codeToApply": {
                "value": dag_run.conf['item']['ClienteleCallNum']
            },
            "descriptionToApply": {
                "value": dag_run.conf['item']['ClienteleCallSummary']
            },
            "startDateToApply": {
                "date": {
                    "year": effective_date.year,
                    "month": effective_date.month,
                    "day": effective_date.day
                }
            },
            "clientBillingAllocationMethodToApply": "urn:replicon:client-billing-allocation-method:user-specified",
            "isTimeEntryAllowed": "false",
            "clientAssignmentsSchedulesToApply": {
                "clients":
                client_create_for_each(dag_run)
            },
            "timeAndMaterials": {
                "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
            },
            "resourceProjectAssignmentModifications": {
                "resourcesToAdd": [
                    {
                        "resource": {
                            "department": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":department:1"
                            }
                        }
                    }
                ]
            }
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def client_for_each(dag_run):
    return list(map(lambda code: {
        "client": {
            "code": code,
        },
        "costAllocationPercentage": "100"
    }, dag_run.conf['item']['CustomerCodes']))


def client_create_for_each(dag_run):
    return list(map(lambda code: {
        "client": {
            "code": code,
        },
        "costAllocationPercentage": "100"
    }, dag_run.conf['item']['CustomerCodes']))


def create_task(dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['item']['CallOpenDate'].split('T')[0].strip(), '%Y-%m-%d').date()
    return list(map(lambda data: {
        "taskModificationToApply": {
            "name": data['ProjName'],
            "codeToApply": {
                "value": data['ProjCode']
            },
            "isClosed": "false",
            "timeEntryStartDateToApply": {
                "date": {
                    "year": effective_date.year,
                    "month": effective_date.month,
                    "day": effective_date.day
                }
            },
            "timeAndExpenseEntryTypeToApply": {
                "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
            },
            "resourceTaskAssignmentModifications": {
                "resourceAllocationsToAdd": [
                    {
                        "resource": {
                            "department": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":department:1"
                            }
                        }
                    }
                ]
            },
            "isTimeEntryAllowed": "true"
        }
    }, dag_run.conf['item']['Projects']))


def create_tasks_exist_project(tasklist, dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['item']['CallOpenDate'].split('T')[0].strip(), '%Y-%m-%d').date()
    return {
        "project": {
            "uri": rail.result('get_project_details')['uri']
        },
        "taskHierarchy":
        list(map(lambda data: {
            "taskModificationToApply": {
                "name": data['ProjName'],
                "codeToApply": {
                    "value": data['ProjCode']
                },
                "isClosed": "false",
                "timeEntryStartDateToApply": {
                    "date": {
                        "year": effective_date.year,
                        "month": effective_date.month,
                        "day": effective_date.day
                    }
                },
                "timeAndExpenseEntryTypeToApply": {
                    "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
                },
                "resourceTaskAssignmentModifications": {
                    "resourceAllocationsToAdd": [
                        {
                            "resource": {
                                "department": {
                                    "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":department:1"
                                }
                            }
                        }
                    ]
                },
                "isTimeEntryAllowed": "true"
            }
        }, tasklist)),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def create_tasks_new_project(dag_run):
    return {
        "project": {
            "uri": rail.result('create_project_data')['uri']
        },
        "taskHierarchy":
        create_task(dag_run),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_expense_code():
    return {
        "projectUri": rail.result('get_project_details')['uri'] if rail.result('get_project_details') else rail.result('create_project_data')['uri']

    }


def assign_all_users():
    return {
        "projectUri": rail.result('create_project_data')['uri'],
        "keyValue": {
            "keyUri": "urn:replicon:project-key-value-key:project-team-member-assignment-type",
            "value": {
                "uri": "urn:replicon:project-team-member-assignment-type:automatically-assign-task"
            }
        }
    }


MANDATORY_FIELDS = {
    "project_fields": {
        "ClienteleCallName": "Project Name",
        "ClienteleCallNum": "Prpject Code",
        "ClienteleCallSummary": "Project Description",
        "CallOpenDate": "Project start date"
    }
}


def get_exception_message(dag_run, mandatory_fields):
    missing_fields = []
    for payload_key, log_value in mandatory_fields.items():
        if not dag_run.conf['item'][payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")


def update_billing_rates():
    return {"projectUri": rail.result('create_project_data')['uri'], "billingRateUri": "urn:replicon:project-specific-billing-rate",
            "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"}


def put_billingrates():
    return {
        "projectTeamMemberBillingRate":
        {
            "projectUri": rail.result('create_project_data')['uri'],
            "resourceUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":department:1",
            "billingRateUris": ["urn:replicon:project-specific-billing-rate"],
            "billingRateCopyOptionUri": "urn:replicon:billing-rate-copy-option:do-not-copy-billing-rates-from-client",
            "defaultBillingRateUri": null
        }
    }


def put_expense_code():
    return {
        "projectUri": rail.result('get_project_details')['uri'] if rail.result('get_project_details') else rail.result('create_project_data')['uri'],
        "expenseCodeUris": rail.result('get_expense_code')

    }


def get_task_details():
    return {

        "pageIndex": "1",
        "pageSize": "10000000",
        "projectUris": [
            rail.result('get_project_details')['uri'] if rail.result(
                    'get_project_details') else rail.result('create_project_data')['uri']
        ],
        "taskDataInclusionOptionUris": []


    }


def get_log_data(dag_run):
    return {
        "TransmissionId": dag_run.conf['webhook']['data']['TransmissionId'],
        "EntryDetails": get_entry_data(),
        "JOB_ID": get_dagrun_ecid(dag_run)
    }


def get_entry_data():
    entry_details = rail.load_all_records(rail.result("render_logs_csv"))
    return list(map(lambda details: {
        "InternalId": int(details["InternalId"]),
        "Status": details["Status"],
        "Description": details["Description"],
        "Entrydate": details["Entrydate"]
    }, entry_details))
